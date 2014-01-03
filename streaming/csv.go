package streaming

import (
	"io"
	"os"
	"github.com/boredomist/mixport/mixpanel"
	"encoding/csv"
)

// CSVStreamer writes the records passed on the given chan in a schema-less
// way.
//
// Format is:
//    distinct_id,key,value
func CSVStreamer(name string, records <-chan mixpanel.EventData) {
	fp, err := os.Create(name)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := fp.Close(); err != nil {
			panic(err)
		}
	}()

	writer := csv.NewWriter(io.Writer(fp))

	// Write the header
	writer.Write([]string{"distinct_id", "key", "value"})

	for {
		if record, ok := <-records; !ok {
			break
		} else {
			id := record["distinct_id"].(string)

			for key, value := range record {
				if key == "distinct_id" {
					continue
				}

				writer.Write([]string{id, key, value.(string)})
			}
		}
	}
}
