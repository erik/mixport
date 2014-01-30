package mixpanel

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// The official base URL
const MixpanelBaseURL = "https://data.mixpanel.com/api/2.0/export"

// Key into the EventData map that contains the UUID of this event. Name is
// chosen to make collisions with actual keys very unlikely.
const EventIDKey = "$__$$event_id"

// Mixpanel struct represents a set of credentials used to access the Mixpanel
// API for a particular product.
type Mixpanel struct {
	Product string
	Key     string
	Secret  string
	BaseURL string
}

// EventData is a representation of each individual JSON record spit out of the
// export process.
type EventData map[string]interface{}

// New creates a Mixpanel object with the given API credentials and uses the
// official API URL.
func New(product, key, secret string) *Mixpanel {
	return NewWithURL(product, key, secret, MixpanelBaseURL)
}

// NewWithURL creates a Mixpanel object with the given API credentials and a
// custom Mixpanel API URL.
//
// I doubt this will ever be useful but there you go.
func NewWithURL(product, key, secret, baseURL string) *Mixpanel {
	m := new(Mixpanel)
	m.Product = product
	m.Key = key
	m.Secret = secret
	m.BaseURL = baseURL
	return m
}

// Add the cryptographic signature that Mixpanel API requests require.
//
// Algorithm:
// - join key=value pairs
// - sort the pairs alphabetically
// - appending a secret
// - take MD5 hex digest.
func (m *Mixpanel) addSignature(args *url.Values) {
	hash := md5.New()

	var params []string
	for k, vs := range *args {
		for _, v := range vs {
			params = append(params, fmt.Sprintf("%s=%s", k, v))
		}
	}

	sort.StringSlice(params).Sort()

	io.WriteString(hash, strings.Join(params, "")+m.Secret)
	args.Set("sig", fmt.Sprintf("%x", hash.Sum(nil)))
}

// Generate the initial, base arguments that should be common to all Mixpanel
// API requests being created here.
func (m *Mixpanel) makeArgs(date time.Time) url.Values {
	args := url.Values{}

	args.Set("format", "json")
	args.Set("api_key", m.Key)
	args.Set("expire", fmt.Sprintf("%d", time.Now().Unix()+10000))

	day := date.Format("2006-01-02")

	args.Set("from_date", day)
	args.Set("to_date", day)

	return args
}

// ExportDate downloads event data for the given day and streams the resulting
// transformed JSON blobs as byte strings over the send-only channel passed
// to the function.
//
// Returns the number of records that have been processed during the run and
// possibly an error.
//
// The optional `moreArgs` parameter can be given to add additional URL
// parameters to the API request.
func (m *Mixpanel) ExportDate(date time.Time, output chan<- EventData, moreArgs *url.Values) (int, error) {
	args := m.makeArgs(date)

	if moreArgs != nil {
		for k, vs := range *moreArgs {
			for _, v := range vs {
				args.Add(k, v)
			}
		}
	}

	m.addSignature(&args)

	resp, err := http.Get(fmt.Sprintf("%s?%s", m.BaseURL, args.Encode()))

	if err != nil {
		return 0, fmt.Errorf("%s: download failed: %s", m.Product, err)
	}

	defer resp.Body.Close()

	return m.TransformEventData(resp.Body, output)
}

// TransformEventData reads JSON objects line by line from `input`, performs a
// simple translation, and pipes the result back out through the `output` chan.
//
// The transformation effectively folds the properties map into the top level
// and attaches product information.
//
// Returns the number of records that have been processed during the run and
// possibly an error.
//
// Input : `{"event": "...", "properties": {"k": "v"}}`
// Output: `{"event": "...", "product: "...", "k": "v", ...}`
func (m *Mixpanel) TransformEventData(input io.Reader, output chan<- EventData) (int, error) {
	decoder := json.NewDecoder(input)

	// Don't default all numeric values to float
	decoder.UseNumber()

	// Keep track of the number of records we've processed.
	numLines := 0

	for ; ; numLines++ {
		var ev struct {
			Error      *string
			Event      string
			Properties map[string]interface{}
		}

		if err := decoder.Decode(&ev); err == io.EOF {
			break
		} else if err != nil {
			return numLines, fmt.Errorf("%s: Failed to parse JSON: %s", m.Product, err)
		} else if ev.Error != nil {
			return numLines, fmt.Errorf("%s: API error: %s", m.Product, *ev.Error)
		}

		if id, err := uuid.NewV4(); err == nil {
			ev.Properties[EventIDKey] = id.String()
		} else {
			return numLines, fmt.Errorf("%s: generating UUID failed: %s", m.Product, err)
		}

		ev.Properties["product"] = m.Product
		ev.Properties["event"] = ev.Event

		output <- ev.Properties
	}

	return numLines, nil
}
