package mixpanel

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// The official base URL
const MixpanelBaseURL = "http://mixpanel.com/api"

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
// custom Mixpanel API URL. (I doubt this will ever be useful but there you go)
func NewWithURL(product, key, secret, baseURL string) *Mixpanel {
	m := new(Mixpanel)
	m.Product = product
	m.Key = key
	m.Secret = secret
	m.BaseURL = baseURL
	return m
}

// Add the cryptographic signature that Mixpanel API requests require.
func (m *Mixpanel) addSignature(args *url.Values) {
	hash := md5.New()
	io.WriteString(hash, args.Encode())
	io.WriteString(hash, m.Secret)

	args.Set("sig", string(hash.Sum(nil)))
}

// Generate the initial, base arguments that all Mixpanel API requests use.
func (m *Mixpanel) makeArgs() url.Values {
	args := url.Values{}

	args.Set("format", "json")
	args.Set("api_key", m.Key)
	args.Set("expire", string(time.Now().Unix()+10000))

	return args
}

// ExportDate downloads event data for the given day and streams the resulting
// transformed JSON blobs as byte strings over the send-only channel passed
// to the function.
//
// The optional `moreArgs` parameter can be given to add additional URL
// parameters to the API request.
func (m *Mixpanel) ExportDate(date time.Time, outChan chan<- EventData, moreArgs *url.Values) {
	args := m.makeArgs()

	if moreArgs != nil {
		for k, vs := range *moreArgs {
			for _, v := range vs {
				args.Add(k, v)
			}
		}
	}

	day := date.Format("2006-01-02")
	args.Set("start", day)
	args.Set("end", day)

	m.addSignature(&args)

	resp, err := http.Get(fmt.Sprintf("%s/2.0/export?%s", m.BaseURL, args.Encode()))
	if err != nil {
		panic("XXX handle this: download FAILED")
	}

	type JSONEvent struct {
		event      string
		properties map[string]interface{}
	}

	decoder := json.NewDecoder(resp.Body)
	for {
		var ev JSONEvent
		if err := decoder.Decode(&ev); err == io.EOF {
			break
		} else if err != nil {
			// TODO: handle {"error": "..."} responses more gracefully?
			panic(err)
		}

		// TODO: ensure that distinct_id is present (even though it should be)
		props := ev.properties
		props["product"] = m.Product
		props["event"] = ev.event

		outChan <- props
	}
}
