package config

import (
	"io"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Sentry struct {
	DSN string `yaml:"dsn"`
}

type Config struct {
	Kafka struct {
		BootstrapServer string                 `yaml:"bootstrapServer"`
		GroupID         string                 `yaml:"groupID"`
		Username        string                 `yaml:"username"`
		Password        string                 `yaml:"password"`
		TopicConfigs    []kafkalib.TopicConfig `yaml:"topicConfigs"`
	}

	Snowflake struct {
		AccountID string `yaml:"account"`
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
		Warehouse string `yaml:"warehouse"`
		Region    string `yaml:"region"`
	}

	Reporting struct {
		Sentry *Sentry `yaml:"sentry"`
	}
}

// TODO: Test this function.
func readFileToConfig(pathToConfig string) (*Config, error) {
	file, err := os.Open(pathToConfig)
	defer file.Close()

	if err != nil {
		return nil, err
	}

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(bytes, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
