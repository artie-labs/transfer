package config

import (
	"context"
	"log"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/jessevdk/go-flags"
)

type Settings struct {
	Config         *Config
	VerboseLogging bool
}

// InjectSettingsIntoContext is used for tests ONLY
func InjectSettingsIntoContext(ctx context.Context, settings *Settings) context.Context {
	return context.WithValue(ctx, constants.ConfigKey, settings)
}

func FromContext(ctx context.Context) *Settings {
	settingsVal := ctx.Value(constants.ConfigKey)
	if settingsVal == nil {
		log.Fatalf("failed to grab settings from context")
	}

	settings, isOk := settingsVal.(*Settings)
	if !isOk {
		log.Fatalf("settings in context is not of *config.Settings type")
	}

	return settings
}

// InitializeCfgIntoContext will take the flags and then parse
// loadConfig is optional for testing purposes.
func InitializeCfgIntoContext(ctx context.Context, args []string, loadConfig bool) context.Context {
	var opts struct {
		ConfigFilePath string `short:"c" long:"config" description:"path to the config file"`
		Verbose        bool   `short:"v" long:"verbose" description:"debug logging" optional:"true"`
	}

	_, err := flags.ParseArgs(&opts, args)
	if err != nil {
		log.Fatalf("failed to parse args, err: %v", err)
	}

	var config *Config
	if loadConfig {
		config, err = readFileToConfig(opts.ConfigFilePath)
		if err != nil {
			log.Fatalf("failed to parse config file. Please check your config, err: %v", err)
		}

		err = config.Validate()
		if err != nil {
			log.Fatalf("Failed to validate config, err: %v", err)
		}
	}

	settings := &Settings{
		Config:         config,
		VerboseLogging: opts.Verbose,
	}

	return context.WithValue(ctx, constants.ConfigKey, settings)
}
