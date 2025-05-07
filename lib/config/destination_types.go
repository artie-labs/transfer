package config

import (
	"cmp"
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
)

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
	RoleARN           string `yaml:"roleARN"`
}

type S3Settings struct {
	FolderName         string                   `yaml:"folderName"`
	Bucket             string                   `yaml:"bucket"`
	AwsAccessKeyID     string                   `yaml:"awsAccessKeyID"`
	AwsSecretAccessKey string                   `yaml:"awsSecretAccessKey"`
	AwsRegion          string                   `yaml:"awsRegion"`
	OutputFormat       constants.S3OutputFormat `yaml:"outputFormat"`
	TableNameSeparator string                   `yaml:"tableNameSeparator"`
}

type Snowflake struct {
	AccountID string `yaml:"account"`
	Username  string `yaml:"username"`
	// If pathToPrivateKey is specified, the password field will be ignored
	PathToPrivateKey string `yaml:"pathToPrivateKey,omitempty"`
	Password         string `yaml:"password,omitempty"`
	Role             string `yaml:"role"`
	Warehouse        string `yaml:"warehouse"`
	Region           string `yaml:"region"`
	Host             string `yaml:"host"`
	Application      string `yaml:"application"`

	// ExternalStage configuration
	ExternalStage *ExternalStage `yaml:"externalStage,omitempty"`

	// AdditionalParameters - This will be added to the connection string.
	// Ref: https://docs.snowflake.com/en/sql-reference/parameters
	AdditionalParameters map[string]string `yaml:"additionalParameters,omitempty"`
}

type ExternalStage struct {
	Enabled bool   `yaml:"enabled"`
	Name    string `yaml:"name"`
	// S3 configuration for the external stage
	Bucket string `yaml:"bucket"`

	// Credentials clause is what we will use to authenticate with S3.
	// It can be static credentials or an AWS_ROLE.
	CredentialsClause string `yaml:"credentialsClause,omitempty"`
	Prefix            string `yaml:"prefix"`
}

type Iceberg struct {
	ApacheLivyURL                   string `yaml:"apacheLivyURL"`
	SessionHeartbeatTimeoutInSecond int    `yaml:"sessionHeartbeatTimeoutInSecond"`
	SessionDriverMemory             string `yaml:"sessionDriverMemory"`
	SessionExecutorMemory           string `yaml:"sessionExecutorMemory"`

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
	// Sourced from: https://mvnrepository.com/artifact/software.amazon.s3tables/s3-tables-catalog-for-iceberg-runtime
	RuntimePackageOverride string   `yaml:"runtimePackageOverride,omitempty"`
	SessionJars            []string `yaml:"sessionJars,omitempty"`

	// [SessionConfig] - Additional session configurations that we will specify when creating a new Livy session.
	SessionConfig map[string]string `yaml:"sessionConfig,omitempty"`
}

func (s S3Tables) GetRuntimePackage() string {
	return cmp.Or(s.RuntimePackageOverride, constants.DefaultS3TablesPackage)
}

func (s S3Tables) ApacheLivyConfig() map[string]any {
	config := map[string]any{
		// Used by SparkSQL to interact with Hadoop S3:
		"spark.hadoop.fs.s3a.secret.key": s.AwsSecretAccessKey,
		"spark.hadoop.fs.s3a.access.key": s.AwsAccessKeyID,
		// Used by SparkSQL to interact with S3 Tables:
		"spark.driver.extraJavaOptions":   fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", s.AwsAccessKeyID, s.AwsSecretAccessKey),
		"spark.executor.extraJavaOptions": fmt.Sprintf("-Daws.accessKeyId=%s -Daws.secretAccessKey=%s", s.AwsAccessKeyID, s.AwsSecretAccessKey),
		// These annotations are needed to work with S3 Tables, sourced from: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source-spark.html
		"spark.jars.packages":                            s.GetRuntimePackage(),
		"spark.sql.extensions":                           "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
		"spark.sql.catalog.s3tablesbucket":               "org.apache.iceberg.spark.SparkCatalog",
		"spark.sql.catalog.s3tablesbucket.catalog-impl":  "software.amazon.s3tables.iceberg.S3TablesCatalog",
		"spark.sql.catalog.s3tablesbucket.warehouse":     s.BucketARN,
		"spark.sql.catalog.s3tablesbucket.client.region": s.Region,
	}

	for key, value := range s.SessionConfig {
		config[key] = value
	}

	return config
}

func (s S3Tables) CatalogName() string {
	return "s3tablesbucket"
}
