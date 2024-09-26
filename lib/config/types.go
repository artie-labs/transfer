package config

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Mode string

const (
	History     Mode = "history"
	Replication Mode = "replication"
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
	TopicConfigs    []*kafkalib.TopicConfig `yaml:"topicConfigs"`

	// Optional parameters
	Username        string `yaml:"username,omitempty"`
	Password        string `yaml:"password,omitempty"`
	EnableAWSMSKIAM bool   `yaml:"enableAWSMKSIAM,omitempty"`
	DisableTLS      bool   `yaml:"disableTLS,omitempty"`
}

type SharedDestinationSettings struct {
	// TruncateExceededValues - This will truncate exceeded values instead of replacing it with `__artie_exceeded_value`
	TruncateExceededValues bool `yaml:"truncateExceededValues"`
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

	// Supported destinations
	MSSQL     *MSSQL      `yaml:"mssql,omitempty"`
	BigQuery  *BigQuery   `yaml:"bigquery,omitempty"`
	Snowflake *Snowflake  `yaml:"snowflake,omitempty"`
	Redshift  *Redshift   `yaml:"redshift,omitempty"`
	S3        *S3Settings `yaml:"s3,omitempty"`

	SharedDestinationSettings SharedDestinationSettings `yaml:"sharedDestinationSettings"`

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
