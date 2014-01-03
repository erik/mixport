package main

import (
	"fmt"
	kinesis "github.com/sendgridlabs/go-kinesis"
	"io"
	"os"
)

// Stream records to Kinesis
func kinesisStreamer(records chan []byte) {
	ksis := kinesis.New("", "")

	args := kinesis.NewArgs()
	args.Add("StreamName", "TODO")

	for {
		record, ok := <-records

		if !ok {
			break
		}

		args.Add("PartitionKey", "TODO")
		args.Add("Data", record)

		if _, err := ksis.PutRecord(args); err != nil {
			fmt.Printf("PutRecord err: %v\n", err)
		}
	}
}

// Stream records to local file
func fileStreamer(name string, recordChan chan []byte) {
	fp, err := os.Create(name)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := fp.Close(); err != nil {
			panic(err)
		}
	}()

	nl := []byte{'\n'}
	writer := io.Writer(fp)
	for {
		if record, ok := <-recordChan; ok {
			writer.Write(record)
			writer.Write(nl)
		} else {
			break
		}
	}
}

func main() {
	// TODO: write me
}
