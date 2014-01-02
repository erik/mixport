package mixpanel

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

/// The official base url
const MIXPANEL_BASE_URL = "http://mixpanel.com/api"

type Mixpanel struct {
	Product string
	Key     string
	Secret  string
	BaseUrl string
}

func New(product, key, secret string) *Mixpanel {
	return NewWithUrl(product, key, secret, MIXPANEL_BASE_URL)
}

func NewWithUrl(product, key, secret, baseUrl string) *Mixpanel {
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
	args.Set("expire", string(time.Now().Unix()+10000))

	return args
}

func (m *Mixpanel) ExportDate(date time.Time, outChan chan<- []byte, moreArgs *url.Values) {
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

	resp, err := http.Get(fmt.Sprintf("%s/2.0/export?%s", m.BaseUrl, args.Encode()))
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

		// Send the data to the proper event channel, or create it if it
		// doesn't already exist.
		if eventChan, ok := eventChans[ev.event]; ok {
			eventChan <- ev.properties
		} else {
			eventChans[ev.event] = make(chan map[string]interface{})
			go m.EventHandler(ev.event, eventChans[ev.event], outChan)

			eventChans[ev.event] <- ev.properties
		}
	}

	// Finish off all the handlers
	for _, ch := range eventChans {
		close(ch)
	}
}

// TODO: ensure distinct_id is present
func (m *Mixpanel) EventHandler(event string, jsonChan chan map[string]interface{}, output chan<- []byte) {
	for {
		props, ok := <-jsonChan

		if !ok {
			break
		}

		props["product"] = m.Product
		props["event"] = event

		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.Encode(props)

		output <- buf.Bytes()
	}
}
