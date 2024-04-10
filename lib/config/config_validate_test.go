package config

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"
)

func TestS3Settings_Validate(t *testing.T) {
	testCases := []struct {
		name        string
		s3          *S3Settings
		expectedErr string
	}{
		{
			name:        "nil",
			expectedErr: "s3 settings are nil",
		},
		{
			name:        "empty",
			s3:          &S3Settings{},
			expectedErr: "one of s3 settings is empty",
		},
		{
			name: "missing bucket",
			s3: &S3Settings{
				AwsSecretAccessKey: "foo",
				AwsAccessKeyID:     "bar",
			},
			expectedErr: "one of s3 settings is empty",
		},
		{
			name: "missing aws access key id",
			s3: &S3Settings{
				AwsSecretAccessKey: "foo",
				Bucket:             "bucket",
			},
			expectedErr: "one of s3 settings is empty",
		},
		{
			name: "missing aws secret access key",
			s3: &S3Settings{
				AwsAccessKeyID: "bar",
				Bucket:         "bucket",
			},
			expectedErr: "one of s3 settings is empty",
		},
		{
			name: "missing output format",
			s3: &S3Settings{
				Bucket:             "bucket",
				AwsSecretAccessKey: "foo",
				AwsAccessKeyID:     "bar",
			},
			expectedErr: `invalid s3 output format ""`,
		},
		{
			name: "valid",
			s3: &S3Settings{
				Bucket:             "bucket",
				AwsSecretAccessKey: "foo",
				AwsAccessKeyID:     "bar",
				OutputFormat:       constants.ParquetFormat,
			},
		},
	}

	for _, testCase := range testCases {
		err := testCase.s3.Validate()
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		} else {
			assert.NoError(t, err, testCase.name)
		}
	}
}

func TestCfg_ValidateRedshift(t *testing.T) {
	testCases := []struct {
		name      string
		redshift  *Redshift
		expectErr string
	}{
		{
			name:      "nil",
			redshift:  nil,
			expectErr: "redshift cfg is nil",
		},
		{
			name:      "redshift settings exist, but all empty",
			redshift:  &Redshift{},
			expectErr: "one of redshift settings is empty",
		},
		{
			name: "redshift settings all set (missing port)",
			redshift: &Redshift{
				Host:              "host",
				Database:          "db",
				Username:          "user",
				Password:          "pw",
				Bucket:            "bucket",
				CredentialsClause: "creds",
			},
			expectErr: "redshift invalid port",
		},
		{
			name: "redshift settings all set (neg port)",
			redshift: &Redshift{
				Host:              "host",
				Port:              -500,
				Database:          "db",
				Username:          "user",
				Password:          "pw",
				Bucket:            "bucket",
				CredentialsClause: "creds",
			},
			expectErr: "redshift invalid port",
		},
		{
			name: "redshift settings all set",
			redshift: &Redshift{
				Host:              "host",
				Port:              123,
				Database:          "db",
				Username:          "user",
				Password:          "pw",
				Bucket:            "bucket",
				CredentialsClause: "creds",
			},
		},
	}

	for _, testCase := range testCases {
		cfg := &Config{
			Redshift: testCase.redshift,
			Output:   constants.Redshift,
		}
		err := cfg.ValidateRedshift()
		if testCase.expectErr != "" {
			assert.ErrorContains(t, err, testCase.expectErr, testCase.name)
		} else {
			assert.NoError(t, err, testCase.name)
		}
	}
}
