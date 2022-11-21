package config

import (
	"log"

	"github.com/jessevdk/go-flags"
)

type Settings struct {
	Config         *Config
	VerboseLogging bool
}

var settings *Settings

func GetSettings() *Settings {
	if settings == nil {
		log.Fatal("Settings is empty, we need to initialize.")
	}

	return settings
}

func ParseArgs(args []string) {
	var opts struct {
		ConfigFilePath string `short:"c" long:"config" description:"path to the config file"`
		// TODO test verbose
		Verbose bool `short:"v" long:"verbose" description:"debug logging" optional:"true"`
	}

	if settings != nil {
		return
	}

	_, err := flags.ParseArgs(&opts, args)
	if err != nil {
		log.Fatalf("Failed to parse args, err: %v", err)
	}

	config, err := readFileToConfig(opts.ConfigFilePath)
	if err != nil {
		log.Fatalf("Failed to parse config file. Please check your config, err: %v", err)
	}

	settings = &Settings{
		Config:         config,
		VerboseLogging: opts.Verbose,
	}
}
