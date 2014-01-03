package exports

import (
	"encoding/json"
	"github.com/boredomist/mixport/mixpanel"
	"io"
)

// JSONStreamer writes records to an io.Writer in JSON format line by line,
// simply serializing the JSON directly.
//
// Format is simply: `{"key": "value", ...}`. `value` should never be a map,
// but may be a vector. Most values are scalar.
func JSONStreamer(w io.Writer, records <-chan mixpanel.EventData) {
	encoder := json.NewEncoder(w)

	for record := range records {
		encoder.Encode(record)
	}
}
