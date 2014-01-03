package main

import (
	"code.google.com/p/gcfg"
	"flag"
	"github.com/boredomist/mixport/mixpanel"
	"github.com/boredomist/mixport/streaming"
	"log"
	"path"
	"runtime"
	"sync"
	"time"
)

// Mixpanel API credentials, used by the configuration parser.
type mixpanelCredentials struct {
	Key    string
	Secret string
	Token  string
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

	JSON struct {
		State     bool
		Directory string
	}

	CSV struct {
		State     bool
		Directory string
	}
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
				// TODO: Call kinesis
				// append(chans, make(chan mixpanel.EventData))
			}

			if cfg.JSON.State {
				ch := make(chan mixpanel.EventData)
				chans = append(chans, ch)

				name := path.Join(cfg.JSON.Directory, product+".json")
				go streaming.JSONStreamer(name, ch)
			}

			if cfg.CSV.State {
				ch := make(chan mixpanel.EventData)
				chans = append(chans, ch)

				name := path.Join(cfg.JSON.Directory, product+".csv")
				go streaming.CSVStreamer(name, ch)
			}

			// FIXME: need to be able to change dates
			go client.ExportDate(exportDate, eventData, nil)

			for data := range eventData {
				for _, ch := range chans {
					ch <- data
				}
			}

			for _, ch := range chans {
				close(ch)
			}

			// XXX: It's completely possible for execution to die
			// here before all of the channels have time to finish
			// processing. Need another waitgroup, maybe
		}(product, *creds)
	}

	// Wait for all our goroutines to finish up
	wg.Wait()
}
