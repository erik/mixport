package streaming

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
//    distinct_id,key,value
func CSVStreamer(w io.Writer, records <-chan mixpanel.EventData) {
	writer := csv.NewWriter(w)

	// Write the header
	writer.Write([]string{"distinct_id", "key", "value"})

	for record := range records {
		id := record["distinct_id"].(string)

		for key, value := range record {
			if key == "distinct_id" {
				continue
			}

			// FIXME: This probably doesn't handle nil correctly.
			writer.Write([]string{id, key, fmt.Sprintf("%v", value)})
		}
	}
}
