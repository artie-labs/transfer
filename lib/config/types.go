package config

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/webhooksutil"
)

type Mode string

const (
	History     Mode = "history"
	Replication Mode = "replication"
)

func (m Mode) IsValid() bool {
	return m == History || m == Replication
}

type KafkaClient string

const (
	KafkaGoClient KafkaClient = "kafka-go"
	FranzGoClient KafkaClient = "franz-go"
)

type Sentry struct {
	DSN string `yaml:"dsn"`
}

type SharedDestinationColumnSettings struct {
	// TODO: Deprecate BigQueryNumericForVariableNumeric in favor of UseBigNumericForVariableNumeric
	// BigQueryNumericForVariableNumeric - If enabled, we will use BigQuery's NUMERIC type for variable numeric types.
	BigQueryNumericForVariableNumeric bool `yaml:"bigQueryNumericForVariableNumeric"`
	UseBigNumericForVariableNumeric   bool `yaml:"useBigNumericForVariableNumeric"`
}

func (s SharedDestinationColumnSettings) BigNumericForVariableNumeric() bool {
	return s.UseBigNumericForVariableNumeric || s.BigQueryNumericForVariableNumeric
}

type SharedDestinationSettings struct {
	// TruncateExceededValues - This will truncate exceeded values instead of replacing it with `__artie_exceeded_value`
	TruncateExceededValues bool `yaml:"truncateExceededValues"`
	// ExpandStringPrecision - This will expand the string precision if the incoming data has a higher precision than the destination table.
	// This is only supported by Redshift at the moment.
	ExpandStringPrecision bool                            `yaml:"expandStringPrecision"`
	ColumnSettings        SharedDestinationColumnSettings `yaml:"columnSettings"`
	// TODO: Standardize on this method.
	UseNewStringMethod bool `yaml:"useNewStringMethod"`
	// [EnableMergeAssertion] - This will enable the merge assertion checks for the destination.
	EnableMergeAssertion bool `yaml:"enableMergeAssertion,omitempty"`
	// [SkipBadValues] - If enabled, we'll skip over all bad values (timestamps, integers, etc.) instead of throwing an error.
	// This is a catch-all setting that supersedes the more specific settings below.
	// Currently only supported for Snowflake.
	SkipBadValues bool `yaml:"skipBadValues"`
	// [SkipBadTimestamps] - If enabled, we'll skip over bad timestamp (or alike) values instead of throwing an error.
	// Currently only supported for Snowflake and BigQuery.
	SkipBadTimestamps bool `yaml:"skipBadTimestamps"`
	// [SkipBadIntegers] - If enabled, we'll skip over bad integer values instead of throwing an error.
	// Currently only supported for Snowflake and Redshift.
	SkipBadIntegers bool `yaml:"skipBadIntegers"`
	// [ForceUTCTimezone] - If enabled, for all TimestampNTZ types, we will return TimestampTZ kind. The converters should ensure that the timezone is set to UTC.
	ForceUTCTimezone bool `yaml:"forceUTCTimezone"`
}

type StagingTableReuseConfig struct {
	// Enable staging table reuse with truncation instead of drop
	Enabled bool `yaml:"enabled"`
	// Pattern for reusable staging table names (default: "_staging")
	TableNameSuffix string `yaml:"tableNameSuffix,omitempty"`
}

type Reporting struct {
	Sentry            *Sentry `yaml:"sentry"`
	EmitExecutionTime bool    `yaml:"emitExecutionTime"`
}

type Config struct {
	KafkaClient KafkaClient               `yaml:"kafkaClient,omitempty"`
	Mode        Mode                      `yaml:"mode"`
	Output      constants.DestinationKind `yaml:"outputSource"`
	Queue       constants.QueueKind       `yaml:"queue"`

	// Flush rules
	FlushIntervalSeconds int  `yaml:"flushIntervalSeconds"`
	FlushSizeKb          int  `yaml:"flushSizeKb"`
	BufferRows           uint `yaml:"bufferRows"`

	// Supported message queues
	Kafka *kafkalib.Kafka `yaml:"kafka,omitempty"`

	// Supported destinations
	BigQuery   *BigQuery    `yaml:"bigquery,omitempty"`
	Databricks *Databricks  `yaml:"databricks,omitempty"`
	MSSQL      *MSSQL       `yaml:"mssql,omitempty"`
	MySQL      *MySQL       `yaml:"mysql,omitempty"`
	Postgres   *Postgres    `yaml:"postgres,omitempty"`
	Snowflake  *Snowflake   `yaml:"snowflake,omitempty"`
	Redshift   *Redshift    `yaml:"redshift,omitempty"`
	S3         *S3Settings  `yaml:"s3,omitempty"`
	GCS        *GCSSettings `yaml:"gcs,omitempty"`
	Iceberg    *Iceberg     `yaml:"iceberg,omitempty"`
	MotherDuck *MotherDuck  `yaml:"motherduck,omitempty"`
	Redis      *Redis       `yaml:"redis,omitempty"`
	Clickhouse *Clickhouse  `yaml:"clickhouse,omitempty"`
	SQS        *SQSSettings `yaml:"sqs,omitempty"`

	SharedDestinationSettings SharedDestinationSettings `yaml:"sharedDestinationSettings"`
	StagingTableReuse         *StagingTableReuseConfig  `yaml:"stagingTableReuse,omitempty"`
	Reporting                 Reporting                 `yaml:"reporting"`
	Telemetry                 struct {
		Metrics struct {
			Provider constants.ExporterKind `yaml:"provider"`
			Settings map[string]any         `yaml:"settings,omitempty"`
		}
	}

	// [WebhookSettings] - This will enable the webhook settings for the transfer.
	WebhookSettings *WebhookSettings `yaml:"webhookSettings,omitempty"`
}

type WebhookSettings struct {
	Enabled    bool                `yaml:"enabled"`
	URL        string              `yaml:"url"`
	APIKey     string              `yaml:"apiKey"`
	Properties map[string]any      `yaml:"properties,omitempty"`
	Source     webhooksutil.Source `yaml:"source"`
}
