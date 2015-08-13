package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
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

	"code.google.com/p/gcfg"
	"github.com/erik/mixport/exports"
	"github.com/erik/mixport/mixpanel"
	flag "github.com/ogier/pflag"
	kinesis "github.com/sendgridlabs/go-kinesis"
)

// Mixpanel API credentials, used by the configuration parser.
type mixpanelCredentials struct {
	Key    string
	Secret string
	Token  string
}

// fileExportConfig contains configuration options common to the file export
// streams (CSV and JSON).
type fileExportConfig struct {
	State        bool
	Gzip         bool
	Fifo         bool
	RemoveFailed bool
	Directory    string
}

// columnExportConfig contains configuration options for the CSV with columns
// export type. It is a superset of the more general `fileExportConfig`.
//
// - `Columns` is the path to a JSON file containing the mapping of events to
//   the columns to include in the CSV output.
type columnExportConfig struct {
	fileExportConfig
	Columns string
}

// configFormat is the in-memory representation of the mixport configuration
// file.
//
// - `Product` is Mixpanel API credential information for each product that
//   will be exported.
// - `Kinesis` is access keys and configuration for Amazon Kinesis exporter.
// - `JSON` and `CSV` are the configuration setups for the `JSON` and `CSV`
//   exporters, respectively.
// - `Columns` is the configuration for the `CSV column` export type.
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

// Map containing the names of the product exports that failed. For deletion
// and error reporting purposes.
var failedExports = make(map[string]bool)

func main() {
	flag.Usage = func() {
		fmt.Println(`Usage: mixport [OPTIONS]

Download and transform Mixpanel event data.

Options:
  -h, --help      Display this message.
  -c, --config    Path to configuration file, defaulting to "./mixport.conf"
  -d, --date      Date of data to pull in YYYY/MM/DD, defaulting to yesterday.
  -p, --procs     Maximum number of OS threads for the go runtime to use,
                  defaulting to number of CPUs available on the machine.
  --products      Comma separated list of products to export.
  --prof          Dump pprof info to the named file.
  -r, --range     Specify a date range to pull data for in YYYY/MM/DD-YYYY/MM/DD
                  format.`)
	}

	var (
		configFile  = flag.StringP("config", "c", "./mixport.conf", "")
		dateString  = flag.StringP("date", "d", "", "")
		rangeString = flag.StringP("range", "r", "", "")
		cpuProfile  = flag.String("prof", "", "")
		productList = flag.String("products", "", "")
		maxProcs    = flag.IntP("procs", "p", runtime.NumCPU(), "")
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

	// Do some sanity checking on each of the file configs.
	fileConfigs := []fileExportConfig{cfg.CSV, cfg.JSON, cfg.Columns.fileExportConfig}
	for _, config := range fileConfigs {
		if config.State && config.Fifo && config.RemoveFailed {
			log.Fatalf("Can't have both `fifo=true` and `removefailed=true`")
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

	// Run each individual product export in a new goroutine.
	for product, creds := range products {
		wg.Add(1)
		go exportProduct(exportConfig{product, *creds, exportStart, exportEnd}, &wg)
	}

	// Wait for all our goroutines to finish up
	wg.Wait()

	if len(failedExports) > 0 {
		log.Printf("Finished with errors:")
		for product := range failedExports {
			log.Printf("\t%s", product)
		}
		os.Exit(1)
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
func createExportFile(export exportConfig, conf fileExportConfig, event, ext string) (io.Writer, func()) {
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

	name := path.Join(conf.Directory, export.Product)

	if event != "" {
		name += fmt.Sprintf("-%s", event)
	}

	name += fmt.Sprintf("-%s.%s", stamp, ext)

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

	// Create a function used to clean up any on-disk resources created for
	// the given exportConfig.
	cleanup := func() {
		if conf.Gzip {
			(writer).(*gzip.Writer).Close()
		}

		fp.Close()

		if conf.Fifo {
			os.Remove(name)
		}

		// Make sure bad files get deleted if the export for this
		// product failed.
		if conf.RemoveFailed {
			if _, err := failedExports[export.Product]; err {
				os.Remove(name)
			}
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

	// Simplify the boilerplate of creating multiplexed channels.
	makeChan := func() chan mixpanel.EventData {
		// Using buffered channels so that a slower receiver won't
		// block a quicker one.
		c := make(chan mixpanel.EventData, 100)
		chans = append(chans, c)

		return c
	}

	if cfg.Kinesis.State {
		c := makeChan()
		region := cfg.Kinesis.Region
		if region == "" {
			// default region
			region = "us-east-1"
		}

		auth := kinesis.NewAuth(cfg.Kinesis.Keyid, cfg.Kinesis.Secretkey)
		ksis := kinesis.New(auth, region)

		wg.Add(1)
		go func() {
			defer wg.Done()
			exports.KinesisStreamer(ksis, cfg.Kinesis.Stream, c)
		}()
	}

	if cfg.JSON.State {
		c := makeChan()

		wg.Add(1)
		go func() {
			defer wg.Done()

			writer, cleanup := createExportFile(export, cfg.JSON, "", "json")
			defer cleanup()

			exports.JSONStreamer(writer, c)
		}()
	}

	if cfg.CSV.State {
		c := makeChan()

		wg.Add(1)
		go func() {
			defer wg.Done()

			writer, cleanup := createExportFile(export, cfg.CSV, "", "csv")
			defer cleanup()

			exports.CSVStreamer(writer, c)
		}()
	}

	if cfg.Columns.State {
		fp, err := os.Open(cfg.Columns.Columns)

		if err != nil {
			log.Fatalf("Couldn't open column defintions: %v", err)
		}

		// We expect a single map in the file of the form:
		//   {"product": {"event": ["columns", ...], ...}, ...}
		columns := make(map[string]map[string][]string)

		if err := json.NewDecoder(fp).Decode(&columns); err != nil {
			log.Fatalf("Failed to read column definitions: %v", err)
		}

		fp.Close()

		// Not much sense in consuming the stream if we have no events
		// to actually capture.
		if prodCols, ok := columns[export.Product]; ok {
			c := makeChan()

			wg.Add(1)
			go func() {
				defer wg.Done()

				defs := make(map[string]exports.EventColumnDef)

				for event, cols := range prodCols {
					writer, cleanup := createExportFile(
						export, cfg.Columns.fileExportConfig, event, "csv")

					defs[event] = exports.NewEventColumnDef(writer, cols)

					defer cleanup()
				}

				exports.CSVColumnStreamer(defs, c)
			}()
		}
	}

	go func() {
		defer close(eventData)

		// Keep track of the total number of lines that have been
		// processed.
		total := 0

		// We want it to be start-end inclusive, so add one day to end
		// date.
		end := export.End.AddDate(0, 0, 1)

		for date := export.Start; date.Before(end); date = date.AddDate(0, 0, 1) {
			num, err := client.ExportDate(date, eventData, nil)

			dateStr := date.Format("2006-01-02")

			// Bail out early if one of the exports for this product
			// fails.
			//
			// TODO: Should we keep going and not report error
			//       until the end? Maybe make that configurable.
			if err != nil {
				log.Printf("%s: %s: export failed: %v", dateStr, export.Product, err)
				failedExports[export.Product] = true
				return
			} else if num == 0 {
				log.Printf("%s: %s: no records.", dateStr, export.Product)
			}

			total += num
		}

		log.Printf("%s: %d records.", export.Product, total)
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
