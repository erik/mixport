package streaming

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/boredomist/mixport/mixpanel"
	kinesis "github.com/sendgridlabs/go-kinesis"
)

// KinesisStreamer writes records to a Kinesis Stream
func KinesisStreamer(records <-chan mixpanel.EventData) {
	ksis := kinesis.New("", "")

	args := kinesis.NewArgs()
	args.Add("StreamName", "TODO")

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
