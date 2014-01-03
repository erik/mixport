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
}
