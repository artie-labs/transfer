package config

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/jessevdk/go-flags"
)

type Settings struct {
	Config         Config
	VerboseLogging bool
}

// LoadSettings will take the flags and then parse, loadConfig is optional for testing purposes.
func LoadSettings(args []string, loadConfig bool) (*Settings, error) {
	var opts struct {
		ConfigFilePath string `short:"c" long:"config" description:"path to the config file"`
		Verbose        bool   `short:"v" long:"verbose" description:"debug logging" optional:"true"`
	}

	if _, err := flags.ParseArgs(&opts, args); err != nil {
		return nil, fmt.Errorf("failed to parse args: %w", err)
	}

	settings := &Settings{
		VerboseLogging: opts.Verbose,
	}

	if loadConfig {
		config, err := readFileToConfig(opts.ConfigFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}

		if err = config.Validate(); err != nil {
			return nil, fmt.Errorf("failed to validate config: %w", err)
		}

		if config.SharedDestinationSettings.EncryptionPassphrase != "" {
			encryptionKey, decodeErr := cryptography.DecodePassphrase(config.SharedDestinationSettings.EncryptionPassphrase)
			if decodeErr != nil {
				return nil, fmt.Errorf("failed to decode encryption passphrase: %w", decodeErr)
			}
			config.SharedDestinationSettings.EncryptionKey = encryptionKey
		}

		settings.Config = *config
	}

	return settings, nil
}
