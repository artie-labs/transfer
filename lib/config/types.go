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

type SharedDestinationColumnSettings struct {
	// BigQueryNumericForVariableNumeric - If enabled, we will use BigQuery's NUMERIC type for variable numeric types.
	BigQueryNumericForVariableNumeric bool `yaml:"bigQueryNumericForVariableNumeric"`
}

type SharedDestinationSettings struct {
	// TruncateExceededValues - This will truncate exceeded values instead of replacing it with `__artie_exceeded_value`
	TruncateExceededValues bool `yaml:"truncateExceededValues"`
	// ExpandStringPrecision - This will expand the string precision if the incoming data has a higher precision than the destination table.
	// This is only supported by Redshift at the moment.
	ExpandStringPrecision bool                            `yaml:"expandStringPrecision"`
	ColumnSettings        SharedDestinationColumnSettings `yaml:"columnSettings"`
}

type Reporting struct {
	Sentry            *Sentry `yaml:"sentry"`
	EmitExecutionTime bool    `yaml:"emitExecutionTime"`
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
	Kafka *Kafka `yaml:"kafka,omitempty"`

	// Supported destinations
	BigQuery   *BigQuery   `yaml:"bigquery,omitempty"`
	Databricks *Databricks `yaml:"databricks,omitempty"`
	MSSQL      *MSSQL      `yaml:"mssql,omitempty"`
	Snowflake  *Snowflake  `yaml:"snowflake,omitempty"`
	Redshift   *Redshift   `yaml:"redshift,omitempty"`
	S3         *S3Settings `yaml:"s3,omitempty"`
	Iceberg    *Iceberg    `yaml:"iceberg,omitempty"`

	SharedDestinationSettings SharedDestinationSettings `yaml:"sharedDestinationSettings"`
	Reporting                 Reporting                 `yaml:"reporting"`
	Telemetry                 struct {
		Metrics struct {
			Provider constants.ExporterKind `yaml:"provider"`
			Settings map[string]any         `yaml:"settings,omitempty"`
		}
	}
}
