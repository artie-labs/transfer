package config

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/jessevdk/go-flags"
)

type Settings struct {
	Config           Config
	VerboseLogging   bool
	AES256Encryption *cryptography.AES256Encryption
}

// LoadSettings will take the flags and then parse, loadConfig is optional for testing purposes.
func LoadSettings(args []string, loadConfig bool) (*Settings, error) {
	var opts struct {
		ConfigFilePath string `short:"c" long:"config" description:"path to the config file"`
		Verbose        bool   `short:"v" long:"verbose" description:"debug logging" optional:"true"`
	}

	_, err := flags.ParseArgs(&opts, args)
	if err != nil {
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

		tcs, err := config.TopicConfigs()
		if err != nil {
			return nil, err
		}

		for _, tc := range tcs {
			tc.Load()
		}

		if err = config.Validate(); err != nil {
			return nil, fmt.Errorf("failed to validate config: %w", err)
		}

		settings.Config = *config

		if config.Encryption != nil && config.Encryption.EncryptionKey != "" {
			aes, err := cryptography.NewAES256Encryption(config.Encryption.EncryptionKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create AES256Encryption: %w", err)
			}

			settings.AES256Encryption = &aes
		}
	}

	return settings, nil
}
