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

var configFile string
var dateString string

func init() {
	// XXX: This one goes to 11.
	runtime.GOMAXPROCS(runtime.NumCPU() * 5)

	const (
		confUsage = "path to configuration file"
		dateUsage = "date (YYYY-MM-DD) of data to pull, default is yesterday"
	)

	flag.StringVar(&configFile, "config", defaultConfig, confUsage)
	flag.StringVar(&configFile, "c", defaultConfig, confUsage)
	flag.StringVar(&dateString, "date", defaultDate, dateUsage)
	flag.StringVar(&dateString, "d", defaultDate, dateUsage)
}

func main() {
	flag.Parse()

	cfg := configFormat{}
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
		go func(product string, creds mixpanelCredentials) {
			defer wg.Done()

			client := mixpanel.New(product, creds.Key, creds.Secret)
			eventData := make(chan mixpanel.EventData)

			// We need to mux eventData into multiple channels
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

				wg.Add(1)
				go func(fp *os.File) {
					defer func() {
						fp.Close()

						// Remove named pipes, as they're useless at this point
						if conf.Fifo {
							os.Remove(name)
						}

						wg.Done()
					}()

					var writer io.Writer
					if conf.Gzip {
						writer = gzip.NewWriter(fp)
					} else {
						writer = fp
					}

					streamer(writer, ch)

				}(fp)
			}

			if cfg.JSON.State {
				setupFileExportStream(cfg.JSON, ".json", exports.JSONStreamer)
			}

			if cfg.CSV.State {
				setupFileExportStream(cfg.CSV, ".csv", exports.CSVStreamer)
			}

			go client.ExportDate(exportDate, eventData, nil)

			for data := range eventData {
				for _, ch := range chans {
					ch <- data
				}
			}

			for _, ch := range chans {
				close(ch)
			}
		}(product, *creds)
	}

	// Wait for all our goroutines to finish up
	wg.Wait()
}
