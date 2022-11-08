package config

import (
	"github.com/artie-labs/transfer/lib/kafkalib"
	"gopkg.in/yaml.v3"
	"io"
	"os"
)

type Config struct {
	// TODO: Add more validation to the config part.
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
}

func (c *Config) IsValid() bool {
	if c == nil {
		return false
	}

	return true
}

func ReadFileToConfig(pathToConfig string) (*Config, error) {
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
