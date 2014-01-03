package main

import (
	"code.google.com/p/gcfg"
	"flag"
	_ "github.com/boredomist/mixport/mixpanel"
	_ "github.com/boredomist/mixport/streaming"
)

func main() {
	cfg := struct {
		Product map[string]*struct {
			Key    string
			Secret string
			Token  string
		}

		Kinesis struct {
			keyid     string
			Secretkey string
			Stream    string
			Region    string
		}

		JSON struct {
			Directory string
		}

		Csv struct {
			Directory string
		}
	}{}

	gcfg.ReadFileInto(cfg, "TODO: filename")

	// TODO: write me

	flag.Parse()
}
