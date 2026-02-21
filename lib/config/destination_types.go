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
	// [Priority] - This is used to specify the priority of the BigQuery job. By default, it'll be set to "INTERACTIVE".
	Priority string `yaml:"priority,omitempty"`
	// [Reservation] - If specified, we'll submit SET @@reservation = '<value>'; before merge queries
	// to route them to a specific slot reservation. Format: projects/{project_id}/locations/{location}/reservations/{reservation_name}
	Reservation string `yaml:"reservation,omitempty"`
}

type Databricks struct {
	Host     string `yaml:"host" json:"host"`
	HttpPath string `yaml:"httpPath" json:"httpPath"`
	Port     int    `yaml:"port" json:"port"`
	Catalog  string `yaml:"catalog" json:"catalog"`
	Volume   string `yaml:"volume" json:"volume"`

	// Authentication: exactly one of the following methods must be configured.
	// Option 1: Personal Access Token
	PersonalAccessToken string `yaml:"personalAccessToken,omitempty" json:"personalAccessToken,omitempty"`

	// Option 2: OAuth M2M (machine-to-machine) using a service principal
	ClientID     string `yaml:"clientID,omitempty" json:"clientID,omitempty"`
	ClientSecret string `yaml:"clientSecret,omitempty" json:"clientSecret,omitempty"`
}

type Postgres struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	Username   string `yaml:"username"`
	Password   string `yaml:"password"`
	Database   string `yaml:"database"`
	DisableSSL bool   `yaml:"disableSSL"`
}

type MSSQL struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

type MySQL struct {
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

type GCSSettings struct {
	FolderName string `yaml:"folderName"`
	Bucket     string `yaml:"bucket"`
	// PathToCredentials is _optional_ if you have GOOGLE_APPLICATION_CREDENTIALS set as an env var
	// Links to credentials: https://cloud.google.com/docs/authentication/application-default-credentials#GAC
	PathToCredentials  string                   `yaml:"pathToCredentials"`
	ProjectID          string                   `yaml:"projectID"`
	OutputFormat       constants.S3OutputFormat `yaml:"outputFormat"`
	TableNameSeparator string                   `yaml:"tableNameSeparator"`
}

type Redis struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database int    `yaml:"database"`
	TLS      bool   `yaml:"tls"` // Enable TLS/SSL connection
}

func (r *Redis) Validate() error {
	if r == nil {
		return fmt.Errorf("redis config is nil")
	}

	if r.Host == "" {
		return fmt.Errorf("redis host is empty")
	}

	if r.Port <= 0 {
		return fmt.Errorf("invalid redis port: %d", r.Port)
	}

	if r.Database < 0 {
		return fmt.Errorf("invalid redis database: %d", r.Database)
	}

	return nil
}

type Snowflake struct {
	AccountID string `yaml:"account"`
	Username  string `yaml:"username"`
	// If pathToPrivateKey is specified, the password field will be ignored
	PathToPrivateKey     string `yaml:"pathToPrivateKey,omitempty"`
	Password             string `yaml:"password,omitempty"`
	Role                 string `yaml:"role"`
	Warehouse            string `yaml:"warehouse"`
	Region               string `yaml:"region"`
	Host                 string `yaml:"host"`
	Application          string `yaml:"application"`
	Streaming            bool   `yaml:"streaming"`
	MaxStreamingChannels int    `yaml:"maxStreamingChannels,omitempty"`

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
	Prefix string `yaml:"prefix"`
	// Credentials clause (optional) is what we will use to authenticate with S3.
	// It can be static credentials or an AWS_ROLE.
	CredentialsClause string `yaml:"credentialsClause,omitempty"`
}

type Iceberg struct {
	ApacheLivyURL                   string `yaml:"apacheLivyURL"`
	SessionHeartbeatTimeoutInSecond int    `yaml:"sessionHeartbeatTimeoutInSecond"`
	SessionDriverMemory             string `yaml:"sessionDriverMemory"`
	SessionExecutorMemory           string `yaml:"sessionExecutorMemory"`
	SessionName                     string `yaml:"sessionName"`
	NumberOfSessions                int    `yaml:"numberOfSessions"`

	// These are the supported catalog types for Iceberg:
	S3Tables    *S3Tables    `yaml:"s3Tables,omitempty"`
	RestCatalog *RestCatalog `yaml:"restCatalog,omitempty"`
}

type RestCatalog struct {
	// AWS credentials to write delta files to S3.
	AwsAccessKeyID     string `yaml:"awsAccessKeyID"`
	AwsSecretAccessKey string `yaml:"awsSecretAccessKey"`
	Region             string `yaml:"region"`
	// [Bucket] - This is where all the ephemeral delta files will be stored.
	Bucket       string `yaml:"bucket"`
	BucketSuffix string `yaml:"bucketSuffix"`

	URI string `yaml:"uri"`

	// [Token] - This is a personal access token for the service principal / user.
	Token string `yaml:"token"`

	// [AuthURI] - This is used for OAuth2 M2M authentication.
	AuthURI string `yaml:"authURI"`
	Scope   string `yaml:"scope"`

	Credential string `yaml:"credential"`
	// [Warehouse] - This is the name of your Iceberg catalog.
	Warehouse string `yaml:"warehouse"`
	Prefix    string `yaml:"prefix"`

	// Sourced from: https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark-runtime-3.5_2.12
	RuntimePackageOverride string `yaml:"runtimePackageOverride,omitempty"`
	// [SessionJars] - Additional JAR files to include in the Spark session.
	SessionJars []string `yaml:"sessionJars,omitempty"`
	// [SessionConfig] - Additional session configurations that we will specify when creating a new Livy session.
	SessionConfig map[string]string `yaml:"sessionConfig,omitempty"`
}

func (r RestCatalog) Validate() error {
	if r.URI == "" {
		return fmt.Errorf("rest catalog uri is required")
	}

	if r.Warehouse == "" {
		return fmt.Errorf("rest catalog warehouse is required")
	}

	// Either token or credential should be provided for authentication
	if r.Token == "" && r.Credential == "" {
		return fmt.Errorf("rest catalog requires either token or credential for authentication")
	}

	// Bucket is always required for staging delta files
	if r.Bucket == "" {
		return fmt.Errorf("rest catalog bucket is required for staging files")
	}

	// AWS credentials are required for S3 access
	if r.AwsAccessKeyID == "" {
		return fmt.Errorf("rest catalog awsAccessKeyID is required")
	}
	if r.AwsSecretAccessKey == "" {
		return fmt.Errorf("rest catalog awsSecretAccessKey is required")
	}

	return nil
}

func (r RestCatalog) CatalogName() string {
	return r.Warehouse
}

func (r RestCatalog) GetRuntimePackage() string {
	return cmp.Or(r.RuntimePackageOverride, constants.DefaultIcebergRuntimePackage)
}

// [ApacheLivyConfig] - This is building the catalog configuration to use Iceberg with REST catalog.
// Ref: https://iceberg.apache.org/docs/latest/spark-configuration/#catalog-configuration
func (r RestCatalog) ApacheLivyConfig() map[string]any {
	config := map[string]any{
		// Required for Iceberg Spark runtime:
		"spark.jars.packages": r.GetRuntimePackage(),
		// Required for Iceberg SQL extensions:
		"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

		fmt.Sprintf("spark.sql.catalog.%s", r.Warehouse):           "org.apache.iceberg.spark.SparkCatalog",
		fmt.Sprintf("spark.sql.catalog.%s.type", r.Warehouse):      "rest",
		fmt.Sprintf("spark.sql.catalog.%s.uri", r.Warehouse):       r.URI,
		fmt.Sprintf("spark.sql.catalog.%s.warehouse", r.Warehouse): r.Warehouse,

		// S3 credentials for Hadoop
		"spark.hadoop.fs.s3a.access.key": r.AwsAccessKeyID,
		"spark.hadoop.fs.s3a.secret.key": r.AwsSecretAccessKey,
	}

	if r.Token != "" {
		config[fmt.Sprintf("spark.sql.catalog.%s.token", r.Warehouse)] = r.Token
	}

	if r.Credential != "" {
		config[fmt.Sprintf("spark.sql.catalog.%s.credential", r.Warehouse)] = r.Credential
	}

	if r.Prefix != "" {
		config[fmt.Sprintf("spark.sql.catalog.%s.prefix", r.Warehouse)] = r.Prefix
	}

	if r.AuthURI != "" {
		config[fmt.Sprintf("spark.sql.catalog.%s.oauth2-server-uri", r.Warehouse)] = r.AuthURI
	}
	if r.Scope != "" {
		config[fmt.Sprintf("spark.sql.catalog.%s.scope", r.Warehouse)] = r.Scope
	}

	for key, value := range r.SessionConfig {
		config[key] = value
	}

	return config
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

type MotherDuck struct {
	DucktapeURL string `yaml:"ducktapeUrl"`
	Token       string `yaml:"token"`
}

type Clickhouse struct {
	Addresses  []string `json:"addresses" yaml:"addresses"`
	Username   string   `json:"username" yaml:"username"`
	Password   string   `json:"password" yaml:"password"`
	IsInsecure bool     `json:"isInsecure,omitempty" yaml:"isInsecure,omitempty"`
}

type SQSSettings struct {
	// AWS configuration
	AwsAccessKeyID     string `yaml:"awsAccessKeyID,omitempty"`
	AwsSecretAccessKey string `yaml:"awsSecretAccessKey,omitempty"`
	AwsRegion          string `yaml:"awsRegion"`
	RoleARN            string `yaml:"roleARN,omitempty"` // For IAM role authentication

	// Queue routing
	// QueueURL - If specified, all tables write to this single queue (single queue mode)
	// If empty, each table writes to its own queue named: dbName_schemaName_tableName (per-table mode)
	QueueURL string `yaml:"queueURL,omitempty"`
}

func (s *SQSSettings) Validate() error {
	if s == nil {
		return fmt.Errorf("sqs settings are nil")
	}

	if s.AwsRegion == "" {
		return fmt.Errorf("sqs awsRegion is required")
	}

	// Either static credentials or IAM role must be configured
	hasStaticCreds := s.AwsAccessKeyID != "" && s.AwsSecretAccessKey != ""
	hasRoleARN := s.RoleARN != ""
	if !hasStaticCreds && !hasRoleARN {
		return fmt.Errorf("either awsAccessKeyID and awsSecretAccessKey or roleARN is required")
	}

	return nil
}

// IsSingleQueueMode returns true if all tables should write to a single queue
func (s *SQSSettings) IsSingleQueueMode() bool {
	return s.QueueURL != ""
}
