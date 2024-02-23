package config

import (
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
)

const (
	defaultFlushTimeSeconds = 10
	defaultFlushSizeKb      = 25 * 1024 // 25 mb

	flushIntervalSecondsStart = 5
	flushIntervalSecondsEnd   = 6 * 60 * 60

	bufferPoolSizeStart = 5
	bufferPoolSizeEnd   = 30000
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
	// Comma-separated Kafka servers to port.
	// e.g. host1:port1,host2:port2,...
	// Following kafka's spec mentioned here: https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers
	BootstrapServer string                  `yaml:"bootstrapServer"`
	GroupID         string                  `yaml:"groupID"`
	Username        string                  `yaml:"username"`
	Password        string                  `yaml:"password"`
	EnableAWSMSKIAM bool                    `yaml:"enableAWSMKSIAM"`
	TopicConfigs    []*kafkalib.TopicConfig `yaml:"topicConfigs"`
}

func (k *Kafka) BootstrapServers() []string {
	return strings.Split(k.BootstrapServer, ",")
}

type S3Settings struct {
	OptionalPrefix     string                   `yaml:"optionalPrefix"`
	Bucket             string                   `yaml:"bucket"`
	AwsAccessKeyID     string                   `yaml:"awsAccessKeyID"`
	AwsSecretAccessKey string                   `yaml:"awsSecretAccessKey"`
	OutputFormat       constants.S3OutputFormat `yaml:"outputFormat"`
}

func (s *S3Settings) Validate() error {
	if s == nil {
		return fmt.Errorf("s3 settings are nil")
	}

	if empty := stringutil.Empty(s.Bucket, s.AwsSecretAccessKey, s.AwsAccessKeyID); empty {
		return fmt.Errorf("one of s3 settings is empty")
	}

	if !constants.IsValidS3OutputFormat(s.OutputFormat) {
		return fmt.Errorf("invalid s3 output format, format: %v", s.OutputFormat)
	}

	return nil
}

type Redshift struct {
	Host             string `yaml:"host"`
	Port             int    `yaml:"port"`
	Database         string `yaml:"database"`
	Username         string `yaml:"username"`
	Password         string `yaml:"password"`
	Bucket           string `yaml:"bucket"`
	OptionalS3Prefix string `yaml:"optionalS3Prefix"`
	// https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-authorization.html
	CredentialsClause string `yaml:"credentialsClause"`
	SkipLgCols        bool   `yaml:"skipLgCols"`
}

type SharedDestinationConfig struct {
	UppercaseEscapedNames bool `yaml:"uppercaseEscapedNames"`
}

type SharedTransferConfig struct {
	TypingSettings typing.Settings `yaml:"typingSettings"`
}

type MsSQL struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (m *MsSQL) DSN() string {
	return fmt.Sprintf("sqlserver://%s:%s@%s:%d?database=%s", m.Username, m.Password, m.Host, m.Port, m.Database)
}

type Snowflake struct {
	AccountID   string `yaml:"account"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	Warehouse   string `yaml:"warehouse"`
	Region      string `yaml:"region"`
	Host        string `yaml:"host"`
	Application string `yaml:"application"`
}

func (p *Pubsub) String() string {
	return fmt.Sprintf("project_id=%s, pathToCredentials=%s", p.ProjectID, p.PathToCredentials)
}

func (k *Kafka) String() string {
	// Don't log credentials.
	return fmt.Sprintf("bootstrapServer=%s, groupID=%s, user_set=%v, pass_set=%v",
		k.BootstrapServer, k.GroupID, k.Username != "", k.Password != "")
}

func (c Config) TopicConfigs() ([]*kafkalib.TopicConfig, error) {
	switch c.Queue {
	case constants.Kafka:
		return c.Kafka.TopicConfigs, nil
	case constants.PubSub:
		return c.Pubsub.TopicConfigs, nil
	}

	return nil, fmt.Errorf("unsupported queue: %v", c.Queue)
}

type Mode string

const (
	History     Mode = "history"
	Replication Mode = "replication"
)

func (m Mode) String() string {
	return string(m)
}

type Config struct {
	Mode   Mode                      `yaml:"mode"`
	Output constants.DestinationKind `yaml:"outputSource"`
	Queue  constants.QueueKind       `yaml:"queue"`

	// Flush rules
	FlushIntervalSeconds int  `yaml:"flushIntervalSeconds"`
	FlushSizeKb          int  `yaml:"flushSizeKb"`
	BufferRows           uint `yaml:"bufferRows"`

	// Supported message queues
	Pubsub *Pubsub
	Kafka  *Kafka

	// Shared Transfer settings
	SharedTransferConfig SharedTransferConfig `yaml:"sharedTransferConfig"`

	// Shared destination configuration
	SharedDestinationConfig SharedDestinationConfig `yaml:"sharedDestinationConfig"`

	// Supported destinations
	MsSQL     *MsSQL      `yaml:"mssql"`
	BigQuery  *BigQuery   `yaml:"bigquery"`
	Snowflake *Snowflake  `yaml:"snowflake"`
	Redshift  *Redshift   `yaml:"redshift"`
	S3        *S3Settings `yaml:"s3"`

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
	if err != nil {
		return nil, err
	}

	defer file.Close()

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

	if config.Mode == "" {
		config.Mode = Replication
	}

	return &config, nil
}

func (c Config) ValidateRedshift() error {
	if c.Output != constants.Redshift {
		return fmt.Errorf("output is not redshift, output: %v", c.Output)
	}

	if c.Redshift == nil {
		return fmt.Errorf("redshift cfg is nil")
	}

	if empty := stringutil.Empty(c.Redshift.Host, c.Redshift.Database, c.Redshift.Username,
		c.Redshift.Password, c.Redshift.Bucket, c.Redshift.CredentialsClause); empty {
		return fmt.Errorf("one of redshift settings is empty")
	}

	if c.Redshift.Port <= 0 {
		return fmt.Errorf("redshift invalid port")
	}

	return nil
}

// Validate will check the output source validity
// It will also check if a topic exists + iterate over each topic to make sure it's valid.
// The actual output source (like Snowflake) and CDC parser will be loaded and checked by other funcs.
func (c Config) Validate() error {
	if c.FlushSizeKb <= 0 {
		return fmt.Errorf("config is invalid, flush size pool has to be a positive number, current value: %v", c.FlushSizeKb)
	}

	if !numbers.BetweenEq(numbers.BetweenEqArgs{
		Start:  flushIntervalSecondsStart,
		End:    flushIntervalSecondsEnd,
		Number: c.FlushIntervalSeconds,
	}) {
		return fmt.Errorf("config is invalid, flush interval is outside of our range, seconds: %v, expected start: %v, end: %v",
			c.FlushIntervalSeconds, flushIntervalSecondsStart, flushIntervalSecondsEnd)
	}

	if bufferPoolSizeStart > int(c.BufferRows) {
		return fmt.Errorf("config is invalid, buffer pool is too small, min value: %v, actual: %v", bufferPoolSizeStart, int(c.BufferRows))
	}

	if !constants.IsValidDestination(c.Output) {
		return fmt.Errorf("config is invalid, output: %s is invalid", c.Output)
	}

	switch c.Output {
	case constants.Redshift:
		if err := c.ValidateRedshift(); err != nil {
			return err
		}
	case constants.S3:
		if err := c.S3.Validate(); err != nil {
			return err
		}
	}

	if c.Queue == constants.Kafka {
		if c.Kafka == nil {
			return fmt.Errorf("config is invalid, no kafka topic configs, kafka: %v", c.Kafka)
		}

		// Username and password are not required (if it's within the same VPC or connecting locally
		if array.Empty([]string{c.Kafka.GroupID, c.Kafka.BootstrapServer}) {
			return fmt.Errorf("config is invalid, kafka settings is invalid, kafka: %s", c.Kafka.String())
		}
	}

	if c.Queue == constants.PubSub {
		if c.Pubsub == nil {
			return fmt.Errorf("config is invalid, no pubsub topic configs, pubsub: %v", c.Pubsub)
		}

		if array.Empty([]string{c.Pubsub.ProjectID, c.Pubsub.PathToCredentials}) {
			return fmt.Errorf("config is invalid, pubsub settings is invalid, pubsub: %s", c.Pubsub.String())
		}
	}

	tcs, err := c.TopicConfigs()
	if err != nil {
		return fmt.Errorf("failed to retrieve topic configs: %w", err)
	}

	if len(tcs) == 0 {
		return fmt.Errorf("config is invalid, no topic configs")
	}

	for _, topicConfig := range tcs {
		if err = topicConfig.Validate(); err != nil {
			return fmt.Errorf("config is invalid, topic config is invalid, tc: %s, err: %w", topicConfig.String(), err)
		}

		// History Mode Validation
		if c.Mode == History {
			if topicConfig.DropDeletedColumns {
				return fmt.Errorf("config is invalid, drop deleted columns is not supported in history mode, topic: %s", topicConfig.String())
			}

			if !topicConfig.IncludeDatabaseUpdatedAt {
				return fmt.Errorf("config is invalid, include database updated at is required in history mode, topic: %s", topicConfig.String())
			}
		}

	}

	return nil
}
