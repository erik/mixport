package main

import (
	"fmt"
	kinesis "github.com/sendgridlabs/go-kinesis"
)

// Stream records to Kinesis
func KinesisStreamer(records chan []byte) {
	ksis := kinesis.New("", "")

	args := kinesis.NewArgs()
	args.Add("StreamName", "TODO")
	args.Add("PartitionKey", "foobar")

	for {
		record, ok := <-records

		if !ok {
			break
		}

		args.Add("Data", record)

		if _, err := ksis.PutRecord(args); err != nil {
			fmt.Printf("PutRecord err: %v\n", err)
		}
	}

}

// Stream records to local file
func FileStreamer(file string, recordChan chan []byte) {
	// TODO: write me
}

func main() {
	// TODO: write me
}
