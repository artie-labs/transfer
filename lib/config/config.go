package config

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

var validOutputSources = []string{"snowflake", "test"}

type Sentry struct {
	DSN string `yaml:"dsn"`
}

type Kafka struct {
	BootstrapServer string                  `yaml:"bootstrapServer"`
	GroupID         string                  `yaml:"groupID"`
	Username        string                  `yaml:"username"`
	Password        string                  `yaml:"password"`
	TopicConfigs    []*kafkalib.TopicConfig `yaml:"topicConfigs"`
}

func (k *Kafka) String() string {
	// Don't log credentials.
	return fmt.Sprintf("bootstrapServer=%s, groupID=%s, user_set=%v, pass_set=%v",
		k.BootstrapServer, k.GroupID, k.Username != "", k.Password != "")
}

type Config struct {
	Output    string `yaml:"outputSource"`
	Kafka     *Kafka
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

// Validate will check the output source validity
// It will also check if a topic exists + iterate over each topic to make sure it's valid.
// The actual output source (like Snowflake) and CDC parser will be loaded and checked by other funcs.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("config is nil")
	}

	// Output sources
	if !array.StringContains(validOutputSources, c.Output) {
		return fmt.Errorf("output: %s is invalid, the valid sources are: %v", c.Output, validOutputSources)
	}

	// TopicConfigs
	if c.Kafka == nil || len(c.Kafka.TopicConfigs) == 0 {
		return fmt.Errorf("no kafka topic configs, kafka: %v", c.Kafka)
	}

	for _, topicConfig := range c.Kafka.TopicConfigs {
		if valid := topicConfig.Valid(); !valid {
			return fmt.Errorf("topic config is invalid, tc: %s", topicConfig.String())
		}
	}

	// Kafka config
	// Username and password are not required (if it's within the same VPC or connecting locally
	if array.Empty([]string{c.Kafka.GroupID, c.Kafka.BootstrapServer}) {
		return fmt.Errorf("kafka settings is invalid, kafka: %s", c.Kafka.String())
	}

	return nil
}
