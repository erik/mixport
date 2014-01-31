package exports

import (
	"encoding/json"
	"github.com/erik/mixport/mixpanel"
	"io"
)

// JSONStreamer writes records to an io.Writer in JSON format line by line,
// simply serializing the JSON directly.
//
// Format is simply: `{"key": "value", ...}`. `value` is usually scalar, but
// can be any valid JSON type.
func JSONStreamer(w io.Writer, records <-chan mixpanel.EventData) {
	encoder := json.NewEncoder(w)

	for record := range records {
		encoder.Encode(record)
	}
}
