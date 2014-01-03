package exports

import (
	"encoding/csv"
	"fmt"
	"github.com/boredomist/mixport/mixpanel"
	"io"
)

// CSVStreamer writes the records passed on the given chan in a schema-less
// way. An initial header row containing the names of the columns is written
// first.
//
// Format is:
//    event_id,key,value
//
// This way, it is possible to GROUP BY event_id to get the full view of a
// single event.
//
// The reason for this format is because it is not possible to know all of the
// column names beforehand, and making multiple passes over the data to find a
// common set of columns is a nonstarter because of the time and memory
// requirements this requires.
func CSVStreamer(w io.Writer, records <-chan mixpanel.EventData) {
	writer := csv.NewWriter(w)

	// Write the header
	writer.Write([]string{"event_id", "key", "value"})

	for record := range records {
		id := record[mixpanel.EventIDKey].(string)

		for key, value := range record {
			if key == mixpanel.EventIDKey {
				continue
			}

			// FIXME: This probably doesn't handle nil correctly.
			writer.Write([]string{id, key, fmt.Sprintf("%v", value)})
		}
	}
}
