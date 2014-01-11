package main

import (
	"code.google.com/p/gcfg"
	"compress/gzip"
	"encoding/json"
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

// TODO: document me
//
// - `Columns` is the path to a JSON file containing the columns specified by
//   stuff and stuff. TODO words.
type columnExportConfig struct {
	fileExportConfig
	Columns string
}

// configFormat is the in-memory representation of the mixport configuration
// file.
//
// - `Product` is Mixpanel API credential information for each product that will be
//   exported.
// - `Kinesis` is access keys and configuration for Amazon Kinesis exporter.
// - `JSON` and `CSV` are the configuration setups for the `JSON` and `CSV`
//   exporters, respectively.
// - `Columns` TODO: words
type configFormat struct {
	Product map[string]*mixpanelCredentials
	Kinesis struct {
		State     bool
		Keyid     string
		Secretkey string
		Stream    string
		Region    string
	}

	JSON    fileExportConfig
	CSV     fileExportConfig
	Columns columnExportConfig
}

// exportConfig simply bundles together the variables describing the export of
// a single Mixpanel product to reduce a bit of noise in helper function type
// signatures.
type exportConfig struct {
	Product    string
	Creds      mixpanelCredentials
	Start, End time.Time
}

// Holds parsed configuration file
var cfg = configFormat{}

// Will be set to true when an export goroutine fails (to set exit status).
var exportFailed = false

func main() {

	var (
		configFile  = flag.StringP("config", "c", "./mixport.conf", "path to configuration file")
		dateString  = flag.StringP("date", "d", "", "date of data to pull in YYYY/MM/DD, default is yesterday")
		rangeString = flag.StringP("range", "r", "", "date range to pull in YYYY/MM/DD-YYYY/MM/DD")
		cpuProfile  = flag.String("prof", "", "dump pprof info to a file.")
		productList = flag.String("products", "", "comma separated list of products to export.")
		maxProcs    = flag.IntP("procs", "p", runtime.NumCPU(), "maximum number of OS threads to spawn.")
	)

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
		go exportProduct(exportConfig{product, *creds, exportStart, exportEnd}, &wg)
	}

	// Wait for all our goroutines to finish up
	wg.Wait()

	if exportFailed {
		log.Fatalf("Export finished with errors.")
	}
}

// createExportFile abstracts the handling of configuration variables common to
// `csv`, `json`, and `columns` into a single function.
//
// The returned tuple contains:
//   - A generic io.Writer which will be passed off to the export function.
//   - A function taking no arguments which should be called after the export
//     function finishes (using defer) to do any necessary cleanup, depending
//     on the specified configuration options.
func createExportFile(export exportConfig, conf fileExportConfig, ext string) (io.Writer, func()) {
	if conf.Gzip {
		ext += ".gz"
	}

	start, end := export.Start, export.End

	timeFmt := "20060102"
	stamp := start.Format(timeFmt)

	// Append end date to timestamp if we're using a date range.
	if start != end {
		stamp += fmt.Sprintf("-%s", end.Format(timeFmt))
	}

	name := path.Join(conf.Directory, fmt.Sprintf("%s-%s.%s", export.Product, stamp, ext))

	if conf.Fifo {
		if err := syscall.Mkfifo(name, syscall.S_IRWXU); err != nil {
			log.Fatalf("Couldn't create named pipe: %s", err)
		}
	}

	fp, err := os.Create(name)
	if err != nil {
		log.Fatalf("Couldn't create file: %s", err)
	}

	writer := (io.Writer)(fp)
	if conf.Gzip {
		writer = gzip.NewWriter(fp)
	}

	cleanup := func() {
		if conf.Gzip {
			(writer).(*gzip.Writer).Close()
		}

		fp.Close()

		if conf.Fifo {
			os.Remove(name)
		}
	}

	return writer, cleanup
}

// exportProduct is called once for each individual mixpanel product to be
// exported. It starts each export function in its own goroutine and will block
// until all events have been processed.
func exportProduct(export exportConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	client := mixpanel.New(export.Product, export.Creds.Key, export.Creds.Secret)
	eventData := make(chan mixpanel.EventData)

	// We need to mux eventData into multiple channels to ensure all export
	// funcs have a chance to see each event instance.
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
			defer wg.Done()
			exports.KinesisStreamer(ksis, cfg.Kinesis.Stream, ch)
		}()
	}

	if cfg.JSON.State {
		ch := make(chan mixpanel.EventData)
		chans = append(chans, ch)

		wg.Add(1)
		go func() {
			defer wg.Done()

			writer, cleanup := createExportFile(export, cfg.JSON, "json")
			defer cleanup()

			exports.JSONStreamer(writer, ch)
		}()
	}

	if cfg.CSV.State {
		ch := make(chan mixpanel.EventData)
		chans = append(chans, ch)

		wg.Add(1)
		go func() {
			defer wg.Done()

			writer, cleanup := createExportFile(export, cfg.CSV, "csv")
			defer cleanup()

			exports.CSVStreamer(writer, ch)
		}()
	}

	if cfg.Columns.State {
		ch := make(chan mixpanel.EventData)
		chans = append(chans, ch)

		fp, err := os.Open(cfg.Columns.Columns)

		if err != nil {
			log.Fatalf("Couldn't open column defintions: %v", err)
		}

		columns := make(map[string][]string)
		if err := json.NewDecoder(fp).Decode(&columns); err != nil {
			log.Fatalf("Failed to read column definitions: %v", err)
		}

		fp.Close()

		defs := make(map[string]exports.EventDef)

		for event, cols := range columns {
			w, cleanup := createExportFile(export, cfg.Columns.fileExportConfig, "csv")
			defer cleanup()

			defs[event] = exports.NewEventDef(w, cols)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			exports.CSVColumnStreamer(defs, ch)
		}()
	}

	go func() {
		defer close(eventData)

		// We want it to be start-end inclusive, so add one day to end date.
		end := export.End.AddDate(0, 0, 1)

		for date := export.Start; date.Before(end); date = date.AddDate(0, 0, 1) {
			if err := client.ExportDate(date, eventData, nil); err != nil {
				log.Printf("export failed: %v", err)
				exportFailed = true
				break
			}
		}
	}()

	// Multiplex each received event to each of the active export funcs.
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
