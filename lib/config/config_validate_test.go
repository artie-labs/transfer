package config

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"
)

func TestS3Settings_Validate(t *testing.T) {
	{
		// nil
		var s3 *S3Settings
		err := s3.Validate()
		assert.ErrorContains(t, err, "s3 settings are nil")
	}
	{
		// empty
		s3 := &S3Settings{}
		err := s3.Validate()
		assert.ErrorContains(t, err, "one of s3 settings is empty")
	}
	{
		// missing bucket
		s3 := &S3Settings{
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
		}
		err := s3.Validate()
		assert.ErrorContains(t, err, "one of s3 settings is empty")
	}
	{
		// missing aws access key id
		s3 := &S3Settings{
			AwsSecretAccessKey: "foo",
			Bucket:             "bucket",
		}
		err := s3.Validate()
		assert.ErrorContains(t, err, "one of s3 settings is empty")
	}
	{
		// missing aws secret access key
		s3 := &S3Settings{
			AwsAccessKeyID: "bar",
			Bucket:         "bucket",
		}
		err := s3.Validate()
		assert.ErrorContains(t, err, "one of s3 settings is empty")
	}
	{
		// missing output format
		s3 := &S3Settings{
			Bucket:             "bucket",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
		}
		err := s3.Validate()
		assert.ErrorContains(t, err, `invalid s3 output format ""`)
	}
	{
		// valid
		s3 := &S3Settings{
			Bucket:             "bucket",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
			OutputFormat:       constants.ParquetFormat,
		}
		err := s3.Validate()
		assert.NoError(t, err)
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
