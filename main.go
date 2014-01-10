package main

import (
	"code.google.com/p/gcfg"
	"compress/gzip"
	"fmt"
	"github.com/boredomist/mixport/exports"
	"github.com/boredomist/mixport/mixpanel"
	flag "github.com/ogier/pflag"
	kinesis "github.com/sendgridlabs/go-kinesis"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
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

var configFile = flag.StringP("config", "c", "./mixport.conf", "path to configuration file")
var dateString = flag.StringP("date", "d", "", "date of data to pull in YYYY/MM/DD, default is yesterday")
var rangeString = flag.StringP("range", "r", "", "date range to pull in YYYY/MM/DD-YYYY/MM/DD")
var cpuProfile = flag.String("prof", "", "dump pprof info to a file.")
var productList = flag.String("products", "", "comma separated list of products to export.")
var maxProcs = flag.IntP("procs", "p", runtime.NumCPU(), "maximum number of OS threads to spawn.")

// Holds parsed configuration file
var cfg = configFormat{}

// Will be set to true when an export goroutine fails. (to set status correctly)
var exportFailed = false

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*maxProcs)

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if err := gcfg.ReadFileInto(&cfg, *configFile); err != nil {
		log.Fatalf("Failed to load %s: %s", *configFile, err)
	}

	var exportStart, exportEnd time.Time

	// Default to yesterday (should be newest available data)
	if *dateString == "" {
		year, month, day := time.Now().UTC().AddDate(0, 0, -1).Date()

		exportStart = time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
		exportEnd = exportStart
	} else {
		if d, err := time.Parse("2006/01/02", *dateString); err != nil {
			log.Fatalf("Invalid date: %s, should be in YYYY/MM/DD format",
				*dateString)
		} else {
			exportStart = d
			exportEnd = exportStart
		}
	}

	if *rangeString != "" {
		formatError := func() {
			log.Fatalf("Invalid range: %s, should be YYYY/MM/DD-YYYY/MM/DD.", *rangeString)
		}

		parts := strings.Split(*rangeString, "-")

		if len(parts) != 2 {
			formatError()
		}

		var err error

		if exportStart, err = time.Parse("2006/01/02", parts[0]); err != nil {
			formatError()
		}

		if exportEnd, err = time.Parse("2006/01/02", parts[1]); err != nil {
			formatError()
		}

	}

	products := make(map[string]*mixpanelCredentials)

	// If not explicitly specified, export all products in the config
	if *productList == "" {
		products = cfg.Product
	} else {
		for _, name := range strings.Split(*productList, ",") {
			if creds, ok := cfg.Product[name]; ok {
				products[name] = creds
			} else {
				log.Fatalf("Don't have credentials specified for %s", name)
			}
		}
	}

	// WaitGroup will hold the process open until all of the child
	// goroutines have completed execution.
	var wg sync.WaitGroup
	wg.Add(len(cfg.Product))

	for product, creds := range products {
		// Run each individual product in a new thread.
		go exportProduct(exportStart, exportEnd, product, *creds, &wg)
	}

	// Wait for all our goroutines to finish up
	wg.Wait()

	if exportFailed {
		log.Fatalf("Export finished with errors.")
	}
}

func exportProduct(start, end time.Time, product string, creds mixpanelCredentials, wg *sync.WaitGroup) {
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

		if conf.Gzip {
			ext += ".gz"
		}

		timeFmt := "20060102"
		stamp := start.Format(timeFmt)

		// Append end date to timestamp if we're using a date range.
		if start != end {
			stamp += fmt.Sprintf("-%s", end.Format(timeFmt))
		}

		name := path.Join(conf.Directory, fmt.Sprintf("%s-%s.%s", product, stamp, ext))

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
		go setupFileExportStream(cfg.JSON, "json", exports.JSONStreamer)
	}

	if cfg.CSV.State {
		go setupFileExportStream(cfg.CSV, "csv", exports.CSVStreamer)
	}

	go func() {
		defer close(eventData)

		// If the export goroutine panics for some reason, don't tear
		// down the whole process with it.
		defer func() {
			if r := recover(); r != nil {
				log.Printf("%s failed: %v", product, r)
				exportFailed = true
			}
		}()

		// We want it to be start-end inclusive, so add one day to end date.
		inclusiveEnd := end.AddDate(0, 0, 1)

		for date := start; date.Before(inclusiveEnd); date = date.AddDate(0, 0, 1) {
			client.ExportDate(date, eventData, nil)
		}
	}()

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
