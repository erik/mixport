package api

import "time"
import "io"
import "net/http"
import "encoding/json"
import "net/url"
import "crypto/md5"

// url.QueryEscape(string)

/// The official base url
const MIXPANEL_BASE_URL = "http://mixpanel.com/api"

type Mixpanel struct {
	Product string
	Key     string
	Secret  string
	BaseUrl string
}

func NewMixpanel(product, key, secret string) *Mixpanel {
	return NewMixpanelWithUrl(product, key, secret, MIXPANEL_BASE_URL)
}

func NewMixpanelWithUrl(product, key, secret, baseUrl string) *Mixpanel {
	m := new(Mixpanel)
	m.Product = product
	m.Key = key
	m.Secret = secret
	m.BaseUrl = baseUrl
	return m
}

func (m *Mixpanel) AddSignature(args *url.Values) {
	hash := md5.New()
	io.WriteString(hash, args.Encode())
	io.WriteString(hash, m.Secret)

	args.Set("sig", string(hash.Sum(nil)))
}

func (m *Mixpanel) MakeArgs() url.Values {
	args := url.Values{}

	args.Set("format", "json")
	args.Set("api_key", m.Key)

	// XXX: I don't even know what this is supposed to represent.
	args.Set("expire", string(time.Now().Unix()+10000))

	return args
}

func (m *Mixpanel) ExportDate(events []string, date time.Time, tempDir string, moreArgs *url.Values) {
	args := m.MakeArgs()

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

	m.AddSignature(&args)

	eventChans := make(map[string]chan map[string]interface{})

	// XXX: clean up
	resp, err := http.Get(m.BaseUrl + "/2.0/export?" + args.Encode())
	if err != nil {
		panic("XXX handle this. FAILED")
	}

	type JsonEvent struct {
		event      string
		properties map[string]interface{}
	}

	decoder := json.NewDecoder(resp.Body)
	for {
		var ev JsonEvent
		if err := decoder.Decode(&ev); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		// Send the data to the proper event channel or create it if it
		// doesn't already exist.
		if eventChan, ok := eventChans[ev.event]; ok {
			eventChan <- ev.properties
		} else {
			eventChans[ev.event] = make(chan map[string]interface{})
			go m.EventHandler(ev.event, tempDir, eventChans[ev.event])

			eventChans[ev.event] <- ev.properties
		}
	}
}

func (m *Mixpanel) EventHandler(event, tempDir string, jsonChan chan map[string]interface{}) {
	/// XXX: Write me.
}
