package config

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func TestS3Settings_Validate(t *testing.T) {
	{
		// nil
		var s3 *S3Settings
		assert.ErrorContains(t, s3.Validate(), "s3 settings are nil")
	}
	{
		// empty
		s3 := &S3Settings{}
		assert.ErrorContains(t, s3.Validate(), "s3 bucket is empty")
	}
	{
		// no credentials or role ARN
		s3 := &S3Settings{
			Bucket: "bucket",
		}
		assert.ErrorContains(t, s3.Validate(), "either awsAccessKeyID and awsSecretAccessKey or roleARN is required")
	}
	{
		// partial static credentials (missing secret)
		s3 := &S3Settings{
			Bucket:         "bucket",
			AwsAccessKeyID: "bar",
		}
		assert.ErrorContains(t, s3.Validate(), "either awsAccessKeyID and awsSecretAccessKey or roleARN is required")
	}
	{
		// partial static credentials (missing key id)
		s3 := &S3Settings{
			Bucket:             "bucket",
			AwsSecretAccessKey: "foo",
		}
		assert.ErrorContains(t, s3.Validate(), "either awsAccessKeyID and awsSecretAccessKey or roleARN is required")
	}
	{
		// missing output format with static credentials
		s3 := &S3Settings{
			Bucket:             "bucket",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
		}
		assert.ErrorContains(t, s3.Validate(), `invalid s3 output format ""`)
	}
	{
		// missing output format with role ARN
		s3 := &S3Settings{
			Bucket:  "bucket",
			RoleARN: "arn:aws:iam::123456789:role/my-role",
		}
		assert.ErrorContains(t, s3.Validate(), `invalid s3 output format ""`)
	}
	{
		// valid with static credentials
		s3 := &S3Settings{
			Bucket:             "bucket",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
			OutputFormat:       constants.ParquetFormat,
		}
		assert.NoError(t, s3.Validate())
	}
	{
		// valid with role ARN
		s3 := &S3Settings{
			Bucket:       "bucket",
			RoleARN:      "arn:aws:iam::123456789:role/my-role",
			OutputFormat: constants.ParquetFormat,
		}
		assert.NoError(t, s3.Validate())
	}
}

func TestGCSSettings_Validate(t *testing.T) {
	{
		// nil
		var gcs *GCSSettings
		err := gcs.Validate()
		assert.ErrorContains(t, err, "gcs settings are nil")
	}
	{
		// empty
		gcs := &GCSSettings{}
		err := gcs.Validate()
		assert.ErrorContains(t, err, "gcs bucket is empty")
	}
	{
		// missing output format
		gcs := &GCSSettings{
			Bucket:    "bucket",
			ProjectID: "project",
		}
		err := gcs.Validate()
		assert.ErrorContains(t, err, `invalid gcs output format ""`)
	}
	{
		// valid
		gcs := &GCSSettings{
			Bucket:       "bucket",
			ProjectID:    "project",
			OutputFormat: constants.ParquetFormat,
		}
		err := gcs.Validate()
		assert.NoError(t, err)
	}
	{
		// valid with credentials
		gcs := &GCSSettings{
			Bucket:            "bucket",
			ProjectID:         "project",
			PathToCredentials: "/path/to/creds.json",
			OutputFormat:      constants.ParquetFormat,
		}
		err := gcs.Validate()
		assert.NoError(t, err)
	}
}

func TestSQSSettings_Validate(t *testing.T) {
	{
		// nil
		var sqs *SQSSettings
		assert.ErrorContains(t, sqs.Validate(), "sqs settings are nil")
	}
	{
		// missing region
		sqs := &SQSSettings{}
		assert.ErrorContains(t, sqs.Validate(), "sqs awsRegion is required")
	}
	{
		// valid with static credentials
		sqs := &SQSSettings{
			AwsRegion:          "us-east-1",
			AwsAccessKeyID:     "key",
			AwsSecretAccessKey: "secret",
		}
		assert.NoError(t, sqs.Validate())
	}
	{
		// valid with role ARN
		sqs := &SQSSettings{
			AwsRegion: "us-east-1",
			RoleARN:   "arn:aws:iam::123456789:role/my-role",
		}
		assert.NoError(t, sqs.Validate())
	}
	{
		// valid single queue mode
		sqs := &SQSSettings{
			AwsRegion:          "us-east-1",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
			QueueURL:           "https://sqs.us-east-1.amazonaws.com/123456789/my-queue",
		}
		assert.NoError(t, sqs.Validate())
		assert.True(t, sqs.IsSingleQueueMode())
	}
	{
		// per-table mode (empty queueURL)
		sqs := &SQSSettings{
			AwsRegion:          "us-east-1",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
		}
		assert.NoError(t, sqs.Validate())
		assert.False(t, sqs.IsSingleQueueMode())
	}
}

func TestColumnEncryptionKMSConfig_Validate(t *testing.T) {
	{
		// Missing keyARN
		cfg := ColumnEncryptionKMSConfig{
			EncryptedPassphrase: "some-encrypted-dek",
			AwsRegion:           "us-east-1",
		}
		assert.ErrorContains(t, cfg.Validate(), "keyARN is required")
	}
	{
		// Missing encryptedPassphrase
		cfg := ColumnEncryptionKMSConfig{
			KeyARN:    "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			AwsRegion: "us-east-1",
		}
		assert.ErrorContains(t, cfg.Validate(), "encryptedPassphrase is required")
	}
	{
		// All empty
		cfg := ColumnEncryptionKMSConfig{}
		assert.ErrorContains(t, cfg.Validate(), "keyARN is required")
	}
	{
		// Missing awsRegion
		cfg := ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
		}
		assert.ErrorContains(t, cfg.Validate(), "awsRegion is required")
	}
	{
		// Valid with default credential chain
		cfg := ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
			AwsRegion:           "us-east-1",
		}
		assert.NoError(t, cfg.Validate())
	}
	{
		// Only awsAccessKeyID provided (missing awsSecretAccessKey)
		cfg := ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
			AwsRegion:           "us-east-1",
			AwsAccessKeyID:      "AKIAIOSFODNN7EXAMPLE",
		}
		assert.ErrorContains(t, cfg.Validate(), "both awsAccessKeyID and awsSecretAccessKey must be provided together")
	}
	{
		// Only awsSecretAccessKey provided (missing awsAccessKeyID)
		cfg := ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
			AwsRegion:           "us-east-1",
			AwsSecretAccessKey:  "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		}
		assert.ErrorContains(t, cfg.Validate(), "both awsAccessKeyID and awsSecretAccessKey must be provided together")
	}
	{
		// Valid with static credentials
		cfg := ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
			AwsRegion:           "us-east-1",
			AwsAccessKeyID:      "AKIAIOSFODNN7EXAMPLE",
			AwsSecretAccessKey:  "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		}
		assert.NoError(t, cfg.Validate())
	}
	{
		// Valid with role ARN
		cfg := ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
			AwsRegion:           "us-east-1",
			RoleARN:             "arn:aws:iam::123456789012:role/kms-role",
		}
		assert.NoError(t, cfg.Validate())
	}
	{
		// Valid with role ARN and external ID
		cfg := ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
			AwsRegion:           "us-east-1",
			RoleARN:             "arn:aws:iam::123456789012:role/kms-role",
			ExternalID:          "my-external-id",
		}
		assert.NoError(t, cfg.Validate())
	}
}

func TestConfig_Validate_Encryption(t *testing.T) {
	baseCfg := func() Config {
		kafka := &kafkalib.Kafka{
			BootstrapServer: "server",
			GroupID:         "group",
			TopicConfigs: []*kafkalib.TopicConfig{
				{
					Database:         "db",
					TableName:        "table",
					Schema:           "schema",
					Topic:            "topic",
					CDCFormat:        constants.DBZPostgresAltFormat,
					CDCKeyFormat:     "org.apache.kafka.connect.json.JsonConverter",
					ColumnsToEncrypt: []string{"email"},
				},
			},
		}
		return Config{
			Kafka:                kafka,
			FlushIntervalSeconds: 10,
			FlushSizeKb:          5,
			BufferRows:           500,
			Output:               constants.Snowflake,
			Queue:                constants.Kafka,
		}
	}

	{
		// Neither passphrase nor KMS config set
		cfg := baseCfg()
		assert.ErrorContains(t, cfg.Validate(), "encryptionPassphrase or encryptionKMSConfig is required when columnsToEncrypt is specified")
	}
	{
		// Both passphrase and KMS config set
		passphrase, err := cryptography.GeneratePassphrase()
		assert.NoError(t, err)
		cfg := baseCfg()
		cfg.SharedDestinationSettings.EncryptionPassphrase = passphrase
		cfg.SharedDestinationSettings.EncryptionKMSConfig = &ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64",
			AwsRegion:           "us-east-1",
		}
		assert.ErrorContains(t, cfg.Validate(), "encryptionPassphrase and encryptionKMSConfig are mutually exclusive")
	}
	{
		// Valid with passphrase only
		passphrase, err := cryptography.GeneratePassphrase()
		assert.NoError(t, err)
		cfg := baseCfg()
		cfg.SharedDestinationSettings.EncryptionPassphrase = passphrase
		assert.NoError(t, cfg.Validate())
	}
	{
		// Valid with KMS config only
		cfg := baseCfg()
		cfg.SharedDestinationSettings.EncryptionKMSConfig = &ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
			AwsRegion:           "us-east-1",
		}
		assert.NoError(t, cfg.Validate())
	}
	{
		// KMS config with missing keyARN
		cfg := baseCfg()
		cfg.SharedDestinationSettings.EncryptionKMSConfig = &ColumnEncryptionKMSConfig{
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
			AwsRegion:           "us-east-1",
		}
		assert.ErrorContains(t, cfg.Validate(), "invalid encryption KMS config: keyARN is required")
	}
	{
		// KMS config with missing encryptedPassphrase
		cfg := baseCfg()
		cfg.SharedDestinationSettings.EncryptionKMSConfig = &ColumnEncryptionKMSConfig{
			KeyARN:    "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			AwsRegion: "us-east-1",
		}
		assert.ErrorContains(t, cfg.Validate(), "invalid encryption KMS config: encryptedPassphrase is required")
	}
	{
		// KMS config with missing awsRegion
		cfg := baseCfg()
		cfg.SharedDestinationSettings.EncryptionKMSConfig = &ColumnEncryptionKMSConfig{
			KeyARN:              "arn:aws:kms:us-east-1:123456789012:key/abcd-1234",
			EncryptedPassphrase: "AQIDAHh-base64-encrypted-dek",
		}
		assert.ErrorContains(t, cfg.Validate(), "invalid encryption KMS config: awsRegion is required")
	}
	{
		// Invalid passphrase (not valid base64 of 32 bytes)
		cfg := baseCfg()
		cfg.SharedDestinationSettings.EncryptionPassphrase = "not-a-valid-passphrase"
		assert.ErrorContains(t, cfg.Validate(), "invalid encryption passphrase")
	}
}

func TestCfg_ValidateRedshift(t *testing.T) {
	{
		// nil
		cfg := &Config{
			Redshift: nil,
			Output:   constants.Redshift,
		}
		err := cfg.ValidateRedshift()
		assert.ErrorContains(t, err, "cfg for Redshift is nil")
	}
	{
		// redshift settings exist, but all empty
		cfg := &Config{
			Redshift: &Redshift{},
			Output:   constants.Redshift,
		}
		err := cfg.ValidateRedshift()
		assert.ErrorContains(t, err, "one of Redshift settings is empty")
	}
	{
		// redshift settings all set (missing port)
		cfg := &Config{
			Redshift: &Redshift{
				Host:              "host",
				Database:          "db",
				Username:          "user",
				Password:          "pw",
				Bucket:            "bucket",
				CredentialsClause: "creds",
			},
			Output: constants.Redshift,
		}
		err := cfg.ValidateRedshift()
		assert.ErrorContains(t, err, "invalid Redshift port")
	}
	{
		// redshift settings all set (neg port)
		cfg := &Config{
			Redshift: &Redshift{
				Host:              "host",
				Port:              -500,
				Database:          "db",
				Username:          "user",
				Password:          "pw",
				Bucket:            "bucket",
				CredentialsClause: "creds",
			},
			Output: constants.Redshift,
		}
		err := cfg.ValidateRedshift()
		assert.ErrorContains(t, err, "invalid Redshift port")
	}
	{
		// redshift settings all set
		cfg := &Config{
			Redshift: &Redshift{
				Host:              "host",
				Port:              123,
				Database:          "db",
				Username:          "user",
				Password:          "pw",
				Bucket:            "bucket",
				CredentialsClause: "creds",
			},
			Output: constants.Redshift,
		}
		err := cfg.ValidateRedshift()
		assert.NoError(t, err)
	}
}
