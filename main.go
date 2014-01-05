package main

import (
	"code.google.com/p/gcfg"
	"compress/gzip"
	"flag"
	"github.com/boredomist/mixport/exports"
	"github.com/boredomist/mixport/mixpanel"
	kinesis "github.com/sendgridlabs/go-kinesis"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// Mixpanel API credentials, used by the configuration parser.
type mixpanelCredentials struct {
	Key    string
	Secret string
	Token  string
}

// Configuration options common to the file export streams (CSV and JSON)
type fileExportConfig struct {
	State     bool
	Gzip      bool
	Fifo      bool
	Directory string
}

// configFormat is the in-memory representation of the mixport configuration
// file.
//
// - `Product` is Mixpanel API credential information for each product that will be
//   exported.
// - `Kinesis` is access keys and configuration for Amazon Kinesis exporter.
// - `JSON` and `CSV` are the configuration setups for the `JSON` and `CSV`
//   exporters, respectively.
type configFormat struct {
	Product map[string]*mixpanelCredentials
	Kinesis struct {
		State     bool
		Keyid     string
		Secretkey string
		Stream    string
		Region    string
	}

	JSON fileExportConfig
	CSV  fileExportConfig
}

const (
	defaultConfig = "mixport.conf"
	defaultDate   = ""
)

var (
	configFile string
	dateString string
	maxProcs   int
)

func init() {
	const (
		confUsage = "path to configuration file"
		dateUsage = "date (YYYY-MM-DD) of data to pull, default is yesterday"
		procUsage = "maximum number of OS threads to spawn. These will be IO bound."
	)

	// TODO: Tune this.
	defaultProcs := runtime.NumCPU() * 4

	flag.StringVar(&configFile, "config", defaultConfig, confUsage)
	flag.StringVar(&configFile, "c", defaultConfig, confUsage)
	flag.StringVar(&dateString, "date", defaultDate, dateUsage)
	flag.StringVar(&dateString, "d", defaultDate, dateUsage)
	flag.IntVar(&maxProcs, "procs", defaultProcs, procUsage)
	flag.IntVar(&maxProcs, "p", defaultProcs, procUsage)
}

var (
	cfg = configFormat{}
)

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(maxProcs)

	if err := gcfg.ReadFileInto(&cfg, configFile); err != nil {
		log.Fatalf("Failed to load %s: %s", configFile, err)
	}

	var exportDate time.Time

	// Default to yesterday (should be newest available data)
	if dateString == "" {
		exportDate = time.Now().UTC().AddDate(0, 0, -1)
	} else {
		if d, err := time.Parse("2006-01-02", dateString); err != nil {
			log.Fatalf("Invalid date: %s, should be in YYYY-MM-DD format",
				dateString)
		} else {
			exportDate = d
		}
	}

	// WaitGroup will hold the process open until all of the child
	// goroutines have completed execution.
	var wg sync.WaitGroup
	wg.Add(len(cfg.Product))

	for product, creds := range cfg.Product {
		// Run each individual product in a new thread.
		go exportProduct(exportDate, product, *creds, &wg)
	}

	// Wait for all our goroutines to finish up
	wg.Wait()
}

func exportProduct(exportDate time.Time, product string, creds mixpanelCredentials, wg *sync.WaitGroup) {
	defer wg.Done()

	client := mixpanel.New(product, creds.Key, creds.Secret)
	eventData := make(chan mixpanel.EventData)

	// We need to mux eventData into multiple channels to
	// ensure all export funcs have a chance to see each event
	// instance.
	var chans []chan mixpanel.EventData

	if cfg.Kinesis.State {
		ch := make(chan mixpanel.EventData)
		chans = append(chans, ch)

		ksis := kinesis.New(cfg.Kinesis.Keyid, cfg.Kinesis.Secretkey)
		if cfg.Kinesis.Region != "" {
			ksis.Region = "us-east-1"
		}

		wg.Add(1)
		go func() {
			exports.KinesisStreamer(ksis, cfg.Kinesis.Stream, ch)
			defer wg.Done()
		}()
	}

	type streamFunc func(io.Writer, <-chan mixpanel.EventData)

	// Generalize setup of JSON and CSV streams into a single function.
	setupFileExportStream := func(conf fileExportConfig, ext string, streamer streamFunc) {
		wg.Add(1)
		defer wg.Done()

		ch := make(chan mixpanel.EventData)
		chans = append(chans, ch)

		name := path.Join(conf.Directory, product+ext)
		if conf.Gzip {
			name += ".gz"
		}

		if conf.Fifo {
			if err := syscall.Mkfifo(name, syscall.S_IRWXU); err != nil {
				log.Fatalf("Couldn't create named pipe: %s", err)
			}
		}

		fp, err := os.Create(name)
		if err != nil {
			log.Fatalf("Couldn't create file: %s", err)
		}

		if conf.Fifo {
			defer os.Remove(name)
		}
		defer fp.Close()

		writer := (io.Writer)(fp)
		if conf.Gzip {
			writer = gzip.NewWriter(fp)
			defer (writer).(*gzip.Writer).Close()
		} else {
			writer = fp
		}

		streamer(writer, ch)
	}

	if cfg.JSON.State {
		go setupFileExportStream(cfg.JSON, ".json", exports.JSONStreamer)
	}

	if cfg.CSV.State {
		go setupFileExportStream(cfg.CSV, ".csv", exports.CSVStreamer)
	}

	go client.ExportDate(exportDate, eventData, nil)

	// Multiplex each received event to each of the active export
	// functions.
	for data := range eventData {
		for _, ch := range chans {
			ch <- data
		}
	}

	// Closing all the channels will signal the streaming export functions
	// that they've reached the end of the stream and should terminate as
	// soon as the channel is drained.
	for _, ch := range chans {
		close(ch)
	}
}
