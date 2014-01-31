package exports

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/erik/mixport/mixpanel"
	kinesis "github.com/sendgridlabs/go-kinesis"
)

// KinesisStreamer pushes each of the JSON blobs into an Amazon Kinesis stream
// by issuing PutRecord requests for each individual event instance.
//
// Make sure your Kinesis stream has enough shards to handle the incoming data,
// as this can be very noisy.
func KinesisStreamer(ksis *kinesis.Kinesis, stream string, records <-chan mixpanel.EventData) {
	args := kinesis.NewArgs()
	args.Add("StreamName", stream)

	for record := range records {
		var buf bytes.Buffer
		encoder := json.NewEncoder(&buf)
		encoder.Encode(record)
		args.Add("Data", buf.Bytes())

		key := fmt.Sprintf("%v-%v", record["product"], record["event"])
		args.Add("PartitionKey", key)

		if _, err := ksis.PutRecord(args); err != nil {
			fmt.Printf("PutRecord err: %v\n", err)
		}
	}
}
