package config

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/stringutil"
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
	// Mutually exclusive with [EncryptionKMSConfig].
	EncryptionPassphrase string `yaml:"encryptionPassphrase,omitempty"`
	// [EncryptionKMSConfig] - If set, the encryption passphrase will be decrypted at startup using AWS KMS.
	// Mutually exclusive with [EncryptionPassphrase].
	EncryptionKMSConfig *ColumnEncryptionKMSConfig `yaml:"encryptionKMSConfig,omitempty"`
	// [CSVConvertUTF8] - If enabled, we will convert all values to UTF-8 when writing to the staging CSV file.
	CSVConvertUTF8 bool `yaml:"csvConvertUTF8,omitempty"`
}

// BuildEncryptionKey resolves the encryption key from either a plaintext passphrase or a KMS-encrypted passphrase.
// Returns nil if no encryption is configured.
func (s SharedDestinationSettings) BuildEncryptionKey(ctx context.Context) ([]byte, error) {
	if s.EncryptionPassphrase != "" {
		return cryptography.DecodePassphrase(s.EncryptionPassphrase)
	}

	if s.EncryptionKMSConfig != nil {
		kmsCfg := s.EncryptionKMSConfig
		awsCfg, err := kmsCfg.BuildAWSConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config for KMS: %w", err)
		}

		kmsClient := awslib.NewKMSClient(awsCfg)
		passphrase, err := kmsClient.DecryptDataKey(ctx, kmsCfg.EncryptedPassphrase, kmsCfg.KeyARN)
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt encryption passphrase via KMS: %w", err)
		}

		return cryptography.DecodePassphrase(passphrase)
	}

	return nil, nil
}

type ColumnEncryptionKMSConfig struct {
	// [KeyARN] - The ARN of the KMS master key used to encrypt the data encryption key.
	KeyARN string `yaml:"keyARN"`
	// [EncryptedPassphrase] - Base64-encoded encrypted data encryption key (produced by KMS GenerateDataKeyWithoutPlaintext or equivalent).
	EncryptedPassphrase string `yaml:"encryptedPassphrase"`
	// [AwsRegion] - AWS region for the KMS call.
	AwsRegion string `yaml:"awsRegion"`

	// Optional: static credentials for AWS authentication.
	// If not provided, falls back to the default credential chain (env vars, IAM role, etc.).
	AwsAccessKeyID     string `yaml:"awsAccessKeyID,omitempty"`
	AwsSecretAccessKey string `yaml:"awsSecretAccessKey,omitempty"`

	// Optional: assume an IAM role for AWS authentication.
	RoleARN    string `yaml:"roleARN,omitempty"`
	ExternalID string `yaml:"externalID,omitempty"`
}

func (c ColumnEncryptionKMSConfig) Validate() error {
	if stringutil.Empty(c.KeyARN) {
		return fmt.Errorf("keyARN is required")
	}

	if stringutil.Empty(c.EncryptedPassphrase) {
		return fmt.Errorf("encryptedPassphrase is required")
	}

	if stringutil.Empty(c.AwsRegion) {
		return fmt.Errorf("awsRegion is required")
	}

	return nil
}

func (c ColumnEncryptionKMSConfig) BuildAWSConfig(ctx context.Context) (aws.Config, error) {
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(c.AwsRegion))

	if c.AwsAccessKeyID != "" && c.AwsSecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AwsAccessKeyID, c.AwsSecretAccessKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("failed to load AWS config: %w", err)
	}

	if c.RoleARN != "" {
		stsClient := sts.NewFromConfig(cfg)
		assumeRoleOpts := func(o *stscreds.AssumeRoleOptions) {
			if c.ExternalID != "" {
				o.ExternalID = &c.ExternalID
			}
		}
		cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, c.RoleARN, assumeRoleOpts))
	}

	return cfg, nil
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

	// Deprecated: old configs nested company_uuid/pipeline_uuid.source_reader_uuid here.
	// Values are migrated to CompanyUUID/PipelineUUID/SourceReaderUUID automatically on load.
	Properties map[string]any `yaml:"properties,omitempty"`
}

// Temporary: this preserves backward compatibility while rolling out changes to WebhookSettings
func (w *WebhookSettings) Migrate() {
	if w == nil {
		return
	}

	// Lift company_uuid / pipeline_uuid / source_reader_uuid out of the old properties block.
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

	// Old configs set source to a service name (e.g. "transfer"). That field now holds
	// the connector source type (e.g. "postgresql"), so discard legacy service-name values.
	if w.Source == "transfer" || w.Source == "reader" || w.Source == "debezium" {
		w.Source = ""
	}
}
