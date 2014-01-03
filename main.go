package main

import (
	"code.google.com/p/gcfg"
	"flag"
	"github.com/boredomist/mixport/mixpanel"
	"github.com/boredomist/mixport/streaming"
	"log"
	"path"
	"sync"
	"time"
)

const defaultConfig = "mixport.conf"

var configFile string

// configFormat is the in-memory representation of the mixport configuration
// file.
type configFormat struct {
	Product map[string]*struct {
		Key    string
		Secret string
		Token  string
	}

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

func init() {
	const confUsage = "path to configuration file"
	flag.StringVar(&configFile, "config", defaultConfig, confUsage)
	flag.StringVar(&configFile, "c", defaultConfig, confUsage+"(shorthand)")
}

func main() {
	flag.Parse()

	cfg := configFormat{}
	if err := gcfg.ReadFileInto(&cfg, configFile); err != nil {
		log.Fatalf("Failed to load %s: %s", configFile, err)
	}

	var wg sync.WaitGroup

	for product, creds := range cfg.Product {
		wg.Add(1)
		// Run each individual product in a new thread.
		go func() {
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
			go client.ExportDate(time.Now(), eventData, nil)

			for data := range eventData {
				for _, ch := range chans {
					ch <- data
				}
			}

			for _, ch := range chans {
				close(ch)
			}
		}()
	}

	// Wait for all our goroutines to finish up
	wg.Wait()
}
