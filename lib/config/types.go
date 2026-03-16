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

func (m Mode) IsValid() bool {
	return m == History || m == Replication
}

type KafkaClient string

const (
	FranzGoClient KafkaClient = "franz-go"
)

type Sentry struct {
	DSN string `yaml:"dsn"`
}

type SharedDestinationColumnSettings struct {
	// BigNumericForVariableNumeric - If enabled, we will use BigQuery's BIGNUMERIC type for variable numeric types.
	// Note: this field also accepts the legacy YAML key "bigQueryNumericForVariableNumeric" for backward compatibility.
	BigNumericForVariableNumeric bool `yaml:"bigQueryNumericForVariableNumeric,omitempty"`
	// [WriteRawBinaryValues] - If enabled, we will write raw binary values to the destination (e.g. BINARY column type)
	// instead of storing them as Base64 encoded strings.
	WriteRawBinaryValues bool `yaml:"writeRawBinaryValues,omitempty"`
}

type SharedDestinationSettings struct {
	// TruncateExceededValues - This will truncate exceeded values instead of replacing it with `__artie_exceeded_value`
	TruncateExceededValues bool `yaml:"truncateExceededValues,omitempty"`
	// ExpandStringPrecision - This will expand the string precision if the incoming data has a higher precision than the destination table.
	// This is only supported by Redshift at the moment.
	ExpandStringPrecision bool                            `yaml:"expandStringPrecision,omitempty"`
	ColumnSettings        SharedDestinationColumnSettings `yaml:"columnSettings"`
	// TODO: Standardize on this method.
	UseNewStringMethod bool `yaml:"useNewStringMethod,omitempty"`
	// [EnableMergeAssertion] - This will enable the merge assertion checks for the destination.
	EnableMergeAssertion bool `yaml:"enableMergeAssertion,omitempty"`
	// [SkipBadValues] - If enabled, we'll skip over all bad values (timestamps, integers, etc.) instead of throwing an error.
	// This is a catch-all setting that supersedes the more specific settings below.
	// Currently only supported for Snowflake.
	SkipBadValues bool `yaml:"skipBadValues,omitempty"`
	// [SkipBadTimestamps] - If enabled, we'll skip over bad timestamp (or alike) values instead of throwing an error.
	// Currently only supported for Snowflake and BigQuery.
	SkipBadTimestamps bool `yaml:"skipBadTimestamps,omitempty"`
	// [SkipBadIntegers] - If enabled, we'll skip over bad integer values instead of throwing an error.
	// Currently only supported for Snowflake and Redshift.
	SkipBadIntegers bool `yaml:"skipBadIntegers,omitempty"`
	// [ForceUTCTimezone] - If enabled, for all TimestampNTZ types, we will return TimestampTZ kind. The converters should ensure that the timezone is set to UTC.
	ForceUTCTimezone bool `yaml:"forceUTCTimezone,omitempty"`
	// [EncryptionPassphrase] - This is used to encrypt columns that should be written to the destination.
	EncryptionPassphrase string `yaml:"encryptionPassphrase,omitempty"`
	// [CSVConvertUTF8] - If enabled, we will convert all values to UTF-8 when writing to the staging CSV file.
	CSVConvertUTF8 bool `yaml:"csvConvertUTF8,omitempty"`
}

type StagingTableReuseConfig struct {
	// Enable staging table reuse with truncation instead of drop
	Enabled bool `yaml:"enabled"`
	// Pattern for reusable staging table names (default: "_staging")
	TableNameSuffix string `yaml:"tableNameSuffix,omitempty"`
}

type Reporting struct {
	Sentry              *Sentry `yaml:"sentry"`
	EmitExecutionTime   bool    `yaml:"emitExecutionTime"`
	EmitDBExecutionTime bool    `yaml:"emitDBExecutionTime"`
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
	Enabled          bool   `yaml:"enabled"`
	URL              string `yaml:"url"`
	APIKey           string `yaml:"apiKey"`
	CompanyUUID      string `yaml:"companyUUID"`
	PipelineUUID     string `yaml:"pipelineUUID,omitempty"`
	SourceReaderUUID string `yaml:"sourceReaderUUID,omitempty"`
	Source           string `yaml:"source,omitempty"`      // connector source type, e.g. "postgresql"
	Destination      string `yaml:"destination,omitempty"` // connector destination type, e.g. "bigquery"
	Mode             string `yaml:"mode,omitempty"`        // transfer run mode, e.g. "replication"

	// Deprecated: old configs nested company_uuid/pipeline_uuid here.
	// Values are migrated to CompanyUUID/PipelineUUID automatically on load.
	Properties map[string]any `yaml:"properties,omitempty"`
}

// oldServiceNames are values that old configs stored in the "source" YAML key to identify
// the Artie service. The field's meaning changed to "connector source type" (e.g. "postgresql"),
// so these stale values are discarded during migration.
var oldServiceNames = map[string]bool{
	"transfer": true,
	"reader":   true,
	"debezium": true,
}

// Temporary: this promotes values from the deprecated Properties map into typed fields,
// and discards stale "source" values that refer to the old service identifier.
func (w *WebhookSettings) migrate(mode Mode) {
	if w == nil {
		return
	}

	// Lift company_uuid / pipeline_uuid out of the old properties block.
	if len(w.Properties) > 0 {
		if w.CompanyUUID == "" {
			if v, ok := w.Properties["company_uuid"].(string); ok {
				w.CompanyUUID = v
			}
		}
		if w.PipelineUUID == "" {
			if v, ok := w.Properties["pipeline_uuid"].(string); ok {
				w.PipelineUUID = v
			}
		}
		if w.SourceReaderUUID == "" {
			if v, ok := w.Properties["source_reader_uuid"].(string); ok {
				w.SourceReaderUUID = v
			}
		}
	}

	if w.Mode == "" {
		w.Mode = mode.String()
	}

	// Old configs set source to a service name (e.g. "transfer"). That field now holds
	// the connector source type (e.g. "postgresql"), so discard legacy service-name values.
	if oldServiceNames[w.Source] {
		w.Source = ""
	}
}
