package config

import (
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
)

const (
	defaultFlushTimeSeconds = 10
	defaultFlushSizeKb      = 25 * 1024 // 25 mb
	defaultBufferPoolSize   = 30000
	bufferPoolSizeMin       = 5

	FlushIntervalSecondsMin = 5
	FlushIntervalSecondsMax = 6 * 60 * 60
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
	Username        string                  `yaml:"username,omitempty"`
	Password        string                  `yaml:"password,omitempty"`
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
		return fmt.Errorf("invalid s3 output format %q", s.OutputFormat)
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
}

type SharedTransferConfig struct {
	TypingSettings typing.Settings `yaml:"typingSettings"`
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
	case constants.Kafka, constants.Reader:
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
	Pubsub *Pubsub `yaml:"pubsub,omitempty"`
	Kafka  *Kafka  `yaml:"kafka,omitempty"`

	// Shared Transfer settings
	SharedTransferConfig SharedTransferConfig `yaml:"sharedTransferConfig"`

	// Supported destinations
	MSSQL     *MSSQL      `yaml:"mssql,omitempty"`
	BigQuery  *BigQuery   `yaml:"bigquery,omitempty"`
	Snowflake *Snowflake  `yaml:"snowflake,omitempty"`
	Redshift  *Redshift   `yaml:"redshift,omitempty"`
	S3        *S3Settings `yaml:"s3,omitempty"`

	Reporting struct {
		Sentry *Sentry `yaml:"sentry"`
	}

	Telemetry struct {
		Metrics struct {
			Provider constants.ExporterKind `yaml:"provider"`
			Settings map[string]any         `yaml:"settings,omitempty"`
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
		config.BufferRows = defaultBufferPoolSize
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
		return fmt.Errorf("output is not Redshift, output: %v", c.Output)
	}

	if c.Redshift == nil {
		return fmt.Errorf("cfg for Redshift is nil")
	}

	if empty := stringutil.Empty(c.Redshift.Host, c.Redshift.Database, c.Redshift.Username,
		c.Redshift.Password, c.Redshift.Bucket, c.Redshift.CredentialsClause); empty {
		return fmt.Errorf("one of Redshift settings is empty")
	}

	if c.Redshift.Port <= 0 {
		return fmt.Errorf("invalid Redshift port")
	}

	return nil
}

// Validate will check the output source validity
// It will also check if a topic exists + iterate over each topic to make sure it's valid.
// The actual output source (like Snowflake) and CDC parser will be loaded and checked by other funcs.
func (c Config) Validate() error {
	if c.FlushSizeKb <= 0 {
		return fmt.Errorf("flush size pool has to be a positive number, current value: %v", c.FlushSizeKb)
	}

	if !numbers.BetweenEq(FlushIntervalSecondsMin, FlushIntervalSecondsMax, c.FlushIntervalSeconds) {
		return fmt.Errorf("flush interval is outside of our range, seconds: %d, expected start: %d, end: %d",
			c.FlushIntervalSeconds, FlushIntervalSecondsMin, FlushIntervalSecondsMax)
	}

	if bufferPoolSizeMin > int(c.BufferRows) {
		return fmt.Errorf("buffer pool is too small, min value: %d, actual: %d", bufferPoolSizeMin, int(c.BufferRows))
	}

	if !constants.IsValidDestination(c.Output) {
		return fmt.Errorf("invalid destination: %s", c.Output)
	}

	switch c.Output {
	case constants.MSSQL:
		if err := c.ValidateMSSQL(); err != nil {
			return err
		}
	case constants.Redshift:
		if err := c.ValidateRedshift(); err != nil {
			return err
		}
	case constants.S3:
		if err := c.S3.Validate(); err != nil {
			return err
		}
	}

	switch c.Queue {
	case constants.Kafka:
		if c.Kafka == nil {
			return fmt.Errorf("kafka config is nil")
		}

		// Username and password are not required (if it's within the same VPC or connecting locally
		if stringutil.Empty(c.Kafka.GroupID, c.Kafka.BootstrapServer) {
			return fmt.Errorf("kafka group or bootstrap server is empty")
		}
	case constants.PubSub:
		if c.Pubsub == nil {
			return fmt.Errorf("pubsub config is nil")
		}

		if stringutil.Empty(c.Pubsub.ProjectID, c.Pubsub.PathToCredentials) {
			return fmt.Errorf("pubsub projectID or pathToCredentials is empty")
		}
	}

	tcs, err := c.TopicConfigs()
	if err != nil {
		return fmt.Errorf("failed to retrieve topic configs: %w", err)
	}

	if len(tcs) == 0 {
		return fmt.Errorf("no topic configs found")
	}

	for _, topicConfig := range tcs {
		if err = topicConfig.Validate(); err != nil {
			return fmt.Errorf("failed to validate topic config: %w", err)
		}

		// History Mode Validation
		if c.Mode == History {
			if topicConfig.DropDeletedColumns {
				return fmt.Errorf("dropDeletedColumns is not supported in history mode, topic: %s", topicConfig.String())
			}

			if !topicConfig.IncludeDatabaseUpdatedAt {
				return fmt.Errorf("includeDatabaseUpdatedAt is required in history mode, topic: %s", topicConfig.String())
			}
		}

	}

	return nil
}
