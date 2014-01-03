package streaming

import (
	"io"
	"os"
	"encoding/json"
	"github.com/boredomist/mixport/mixpanel"
)

// JSONStreamer writes records to a local file in JSON format line by line,
// simply serializing the JSON directly.
//
// Format is simply: `{"key": "value", ...}`, where all `value`s are scalar
// (i.e. not maps or vectors).
func JSONStreamer(name string, records <-chan mixpanel.EventData) {
	fp, err := os.Create(name)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := fp.Close(); err != nil {
			panic(err)
		}
	}()

	encoder := json.NewEncoder(io.Writer(fp))

	for {
		if record, ok := <-records; !ok {
			break
		} else {
			// FIXME: Does this write newlines?
			encoder.Encode(record)
		}
	}
}
