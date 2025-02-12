package config

import "github.com/artie-labs/transfer/lib/config/constants"

type BigQuery struct {
	// PathToCredentials is _optional_ if you have GOOGLE_APPLICATION_CREDENTIALS set as an env var
	// Links to credentials: https://cloud.google.com/docs/authentication/application-default-credentials#GAC
	PathToCredentials string `yaml:"pathToCredentials"`
	DefaultDataset    string `yaml:"defaultDataset"`
	ProjectID         string `yaml:"projectID"`
	Location          string `yaml:"location"`
}

type Databricks struct {
	Host                string `yaml:"host"`
	HttpPath            string `yaml:"httpPath"`
	Port                int    `yaml:"port"`
	Catalog             string `yaml:"catalog"`
	PersonalAccessToken string `yaml:"personalAccessToken"`
	Volume              string `yaml:"volume"`
}

type MSSQL struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
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

type S3Settings struct {
	FolderName         string                   `yaml:"folderName"`
	Bucket             string                   `yaml:"bucket"`
	AwsAccessKeyID     string                   `yaml:"awsAccessKeyID"`
	AwsSecretAccessKey string                   `yaml:"awsSecretAccessKey"`
	OutputFormat       constants.S3OutputFormat `yaml:"outputFormat"`
}

type Snowflake struct {
	AccountID string `yaml:"account"`
	Username  string `yaml:"username"`
	// If pathToPrivateKey is specified, the password field will be ignored
	PathToPrivateKey string `yaml:"pathToPrivateKey,omitempty"`
	Password         string `yaml:"password,omitempty"`

	Warehouse   string `yaml:"warehouse"`
	Region      string `yaml:"region"`
	Host        string `yaml:"host"`
	Application string `yaml:"application"`
}

type Iceberg struct {
	ApacheLivyURL string `yaml:"apacheLivyURL"`

	// Optional:
	SessionJars []string `yaml:"sessionJars,omitempty"`
	// Current implementation of Iceberg uses S3Tables:
	S3Tables *S3Tables `yaml:"s3Tables,omitempty"`
}

type S3Tables struct {
	AwsAccessKeyID     string `yaml:"awsAccessKeyID"`
	AwsSecretAccessKey string `yaml:"awsSecretAccessKey"`
	BucketARN          string `yaml:"bucketARN"`
	Region             string `yaml:"region"`
	// Bucket - This is where all the ephemeral delta files will be stored.
	Bucket string `yaml:"bucket"`
}
