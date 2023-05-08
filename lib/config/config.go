package config

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/numbers"
	"gopkg.in/yaml.v3"
	"io"
	"os"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

const (
	defaultFlushTimeSeconds = 10
	defaultFlushSizeKb = 900

	flushIntervalSecondsStart = 5
	flushIntervalSecondsEnd   = 6 * 60 * 60

	bufferPoolSizeStart = 5
	// Snowflake has a limit of 2^14 elements within an expression.
	// https://github.com/snowflakedb/snowflake-connector-python/issues/37
	bufferPoolSizeEnd = 15000
)

type Sentry struct {
	DSN string `yaml:"dsn"`
}

type Pubsub struct {
	ProjectID         string                  `yaml:"projectID"`
	TopicConfigs      []*kafkalib.TopicConfig `yaml:"topicConfigs"`
	PathToCredentials string                  `yaml:"pathToCredentials"`
}

type Kafka struct {
	BootstrapServer string                  `yaml:"bootstrapServer"`
	GroupID         string                  `yaml:"groupID"`
	Username        string                  `yaml:"username"`
	Password        string                  `yaml:"password"`
	EnableAWSMSKIAM bool                    `yaml:"enableAWSMKSIAM"`
	TopicConfigs    []*kafkalib.TopicConfig `yaml:"topicConfigs"`
}

type BigQuery struct {
	// PathToCredentials is _optional_ if you have GOOGLE_APPLICATION_CREDENTIALS set as an env var
	// Links to credentials: https://cloud.google.com/docs/authentication/application-default-credentials#GAC
	PathToCredentials string `yaml:"pathToCredentials"`
	DefaultDataset    string `yaml:"defaultDataset"`
	ProjectID         string `yaml:"projectID"`
}

type Snowflake struct {
	AccountID string `yaml:"account"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Warehouse string `yaml:"warehouse"`
	Region    string `yaml:"region"`
	Host      string `yaml:"host"`
}

func (p *Pubsub) String() string {
	return fmt.Sprintf("project_id=%s, pathToCredentials=%s", p.ProjectID, p.PathToCredentials)
}

func (k *Kafka) String() string {
	// Don't log credentials.
	return fmt.Sprintf("bootstrapServer=%s, groupID=%s, user_set=%v, pass_set=%v",
		k.BootstrapServer, k.GroupID, k.Username != "", k.Password != "")
}

type Config struct {
	Output constants.DestinationKind `yaml:"outputSource"`
	Queue  constants.QueueKind       `yaml:"queue"`

	FlushIntervalSeconds int  `yaml:"flushIntervalSeconds"`
	FlushSizeKb          int  `yaml:"flushSizeKb"`
	BufferRows           uint `yaml:"bufferRows"`

	Pubsub *Pubsub
	Kafka  *Kafka

	BigQuery  *BigQuery  `yaml:"bigquery"`
	Snowflake *Snowflake `yaml:"snowflake"`

	Reporting struct {
		Sentry *Sentry `yaml:"sentry"`
	}

	Telemetry struct {
		Metrics struct {
			Provider constants.ExporterKind `yaml:"provider"`
			Settings map[string]interface{} `yaml:"settings,omitempty"`
		}
	}
}

func readFileToConfig(pathToConfig string) (*Config, error) {
	file, err := os.Open(pathToConfig)
	defer file.Close()

	if err != nil {
		return nil, err
	}

	var bytes []byte
	bytes, err = io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(bytes, &config)
	if err != nil {
		return nil, err
	}

	if config.Queue == "" {
		// We default to Kafka for backwards compatibility
		config.Queue = constants.Kafka
	}

	if config.FlushIntervalSeconds == 0 {
		config.FlushIntervalSeconds = defaultFlushTimeSeconds
	}

	if config.BufferRows == 0 {
		config.BufferRows = bufferPoolSizeEnd
	}

	if config.FlushSizeKb == 0 {
		config.FlushSizeKb = defaultFlushSizeKb
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

	if c.FlushSizeKb <= 0 {
		return fmt.Errorf("config is invalid, flush size pool has to be a positive number, current value: %v", c.FlushSizeKb)
	}

	if !numbers.BetweenEq(flushIntervalSecondsStart, flushIntervalSecondsEnd, c.FlushIntervalSeconds) {
		return fmt.Errorf("config is invalid, flush interval is outside of our range, seconds: %v, expected start: %v, end: %v",
			c.FlushIntervalSeconds, flushIntervalSecondsStart, flushIntervalSecondsEnd)
	}

	if !numbers.BetweenEq(bufferPoolSizeStart, bufferPoolSizeEnd, int(c.BufferRows)) {
		return fmt.Errorf("config is invalid, buffer pool is outside of our range: %v, expected start: %v, end: %v",
			c.BufferRows, bufferPoolSizeStart, bufferPoolSizeEnd)
	}

	if !constants.IsValidDestination(c.Output) {
		return fmt.Errorf("config is invalid, output: %s is invalid", c.Output)
	}

	if c.Queue == constants.Kafka {
		if c.Kafka == nil || len(c.Kafka.TopicConfigs) == 0 {
			return fmt.Errorf("config is invalid, no kafka topic configs, kafka: %v", c.Kafka)
		}

		for _, topicConfig := range c.Kafka.TopicConfigs {
			if valid := topicConfig.Valid(); !valid {
				return fmt.Errorf("config is invalid, topic config is invalid, tc: %s", topicConfig.String())
			}
		}

		// Username and password are not required (if it's within the same VPC or connecting locally
		if array.Empty([]string{c.Kafka.GroupID, c.Kafka.BootstrapServer}) {
			return fmt.Errorf("config is invalid, kafka settings is invalid, kafka: %s", c.Kafka.String())
		}
	}

	if c.Queue == constants.PubSub {
		if c.Pubsub == nil || len(c.Pubsub.TopicConfigs) == 0 {
			return fmt.Errorf("config is invalid, no pubsub topic configs, pubsub: %v", c.Pubsub)
		}

		for _, topicConfig := range c.Pubsub.TopicConfigs {
			if valid := topicConfig.Valid(); !valid {
				return fmt.Errorf("config is invalid, topic config is invalid, tc: %s", topicConfig.String())
			}
		}

		if array.Empty([]string{c.Pubsub.ProjectID, c.Pubsub.PathToCredentials}) {
			return fmt.Errorf("config is invalid, pubsub settings is invalid, pubsub: %s", c.Pubsub.String())
		}
	}

	return nil
}
