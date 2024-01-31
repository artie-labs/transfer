package config

import (
	"context"
	"fmt"
	"log"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/jessevdk/go-flags"
)

type Settings struct {
	Config         Config
	VerboseLogging bool
}

// InjectSettingsIntoContext is used for tests ONLY
func InjectSettingsIntoContext(ctx context.Context, settings *Settings) context.Context {
	return context.WithValue(ctx, constants.ConfigKey, settings)
}

func FromContext(ctx context.Context) *Settings {
	settingsVal := ctx.Value(constants.ConfigKey)
	if settingsVal == nil {
		log.Panic("Failed to grab settings from context")
	}

	settings, isOk := settingsVal.(*Settings)
	if !isOk {
		log.Panic("Settings in context is not of *config.Settings type")
	}

	return settings
}

// InitializeCfgIntoContext will take the flags and then parse
// loadConfig is optional for testing purposes.
func InitializeCfgIntoContext(ctx context.Context, args []string, loadConfig bool) (context.Context, error) {
	var opts struct {
		ConfigFilePath string `short:"c" long:"config" description:"path to the config file"`
		Verbose        bool   `short:"v" long:"verbose" description:"debug logging" optional:"true"`
	}

	_, err := flags.ParseArgs(&opts, args)
	if err != nil {
		return nil, fmt.Errorf("failed to parse args, err: %w", err)
	}

	settings := &Settings{
		VerboseLogging: opts.Verbose,
	}

	if loadConfig {
		config, err := readFileToConfig(opts.ConfigFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse config file. Please check your config, err: %w", err)
		}

		if err = config.Validate(); err != nil {
			return nil, fmt.Errorf("failed to validate config, err: %w", err)
		}

		settings.Config = *config
	}

	return context.WithValue(ctx, constants.ConfigKey, settings), nil
}
