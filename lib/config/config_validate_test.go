package config

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"
)

func TestS3Settings_Validate(t *testing.T) {
	type _testCase struct {
		Name      string
		S3        *S3Settings
		ExpectErr bool
	}

	testCases := []_testCase{
		{
			Name:      "nil",
			ExpectErr: true,
		},
		{
			Name:      "empty",
			S3:        &S3Settings{},
			ExpectErr: true,
		},
		{
			Name: "missing bucket",
			S3: &S3Settings{
				AwsSecretAccessKey: "foo",
				AwsAccessKeyID:     "bar",
			},
			ExpectErr: true,
		},
		{
			Name: "missing aws access key id",
			S3: &S3Settings{
				AwsSecretAccessKey: "foo",
				Bucket:             "bucket",
			},
			ExpectErr: true,
		},
		{
			Name: "missing aws secret access key",
			S3: &S3Settings{
				AwsAccessKeyID: "bar",
				Bucket:         "bucket",
			},
			ExpectErr: true,
		},
		{
			Name: "missing output format",
			S3: &S3Settings{
				Bucket:             "bucket",
				AwsSecretAccessKey: "foo",
				AwsAccessKeyID:     "bar",
			},
			ExpectErr: true,
		},
		{
			Name: "valid",
			S3: &S3Settings{
				Bucket:             "bucket",
				AwsSecretAccessKey: "foo",
				AwsAccessKeyID:     "bar",
				OutputFormat:       constants.ParquetFormat,
			},
		},
	}

	for _, testCase := range testCases {
		err := testCase.S3.Validate()
		if testCase.ExpectErr {
			assert.Error(t, err, testCase.Name)
		} else {
			assert.NoError(t, err, testCase.Name)
		}
	}
}

func TestCfg_ValidateRedshift(t *testing.T) {
	type _testCase struct {
		Name      string
		Redshift  *Redshift
		ExpectErr bool
	}

	testCases := []_testCase{
		{
			Name:      "nil",
			Redshift:  nil,
			ExpectErr: true,
		},
		{
			Name:      "redshift settings exist, but all empty",
			Redshift:  &Redshift{},
			ExpectErr: true,
		},
		{
			Name: "redshift settings all set (missing port)",
			Redshift: &Redshift{
				Host:              "host",
				Database:          "db",
				Username:          "user",
				Password:          "pw",
				Bucket:            "bucket",
				CredentialsClause: "creds",
			},
			ExpectErr: true,
		},
		{
			Name: "redshift settings all set (neg port)",
			Redshift: &Redshift{
				Host:              "host",
				Port:              -500,
				Database:          "db",
				Username:          "user",
				Password:          "pw",
				Bucket:            "bucket",
				CredentialsClause: "creds",
			},
			ExpectErr: true,
		},
		{
			Name: "redshift settings all set",
			Redshift: &Redshift{
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
			Redshift: testCase.Redshift,
			Output:   constants.Redshift,
		}
		err := cfg.ValidateRedshift()
		if testCase.ExpectErr {
			assert.Error(t, err, testCase.Name)
		} else {
			assert.NoError(t, err, testCase.Name)
		}
	}
}
