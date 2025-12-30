package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"

	"github.com/stretchr/testify/assert"
)

const (
	validKafkaTopic = `
kafka:
 bootstrapServer: kafka:9092
 groupID: 123
 username: foo
 password: bar
 topicConfigs:
  - { db: customer, tableName: orders, schema: public, topic: orders, cdcFormat: debezium.mongodb, cdcKeyFormat: org.apache.kafka.connect.json.JsonConverter}
  - { db: customer, tableName: customer, schema: public, topic: customer, cdcFormat: debezium.mongodb, cdcKeyFormat: org.apache.kafka.connect.storage.StringConverter}
`
)

func TestBigQuery_DSN(t *testing.T) {
	b := BigQuery{
		DefaultDataset: "dataset",
		ProjectID:      "project",
	}

	assert.Equal(t, "bigquery://project/dataset", b.DSN())
	b.Location = "eu"
	assert.Equal(t, "bigquery://project/eu/dataset", b.DSN())
}

func TestReadNonExistentFile(t *testing.T) {
	_, err := readFileToConfig(filepath.Join(t.TempDir(), "213213231312"))
	assert.ErrorContains(t, err, "no such file or directory")
}

func TestOutputSourceValid(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_output_source_valid", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.NoError(t, err)

	defer file.Close()

	_, err = io.WriteString(file, fmt.Sprintf(`
outputSource: snowflake
flushIntervalSeconds: 15
bufferRows: 10
%s
`, validKafkaTopic))
	assert.NoError(t, err)

	config, err := readFileToConfig(randomFile)
	assert.NoError(t, err)

	assert.Equal(t, config.FlushIntervalSeconds, 15)
	assert.Equal(t, int(config.BufferRows), 10)

	tcs := config.TopicConfigs()
	assert.Len(t, tcs, 2)
	assert.NoError(t, config.Validate())

	// Now let's unset Kafka.
	config.Kafka.GroupID = ""
	config.Kafka.BootstrapServer = ""
	assert.ErrorContains(t, config.Validate(), "kafka group or bootstrap server is empty")

	// Now if it's Reader, then it's fine.
	config.Queue = constants.Reader
	assert.NoError(t, config.Validate())
}

func TestOutputSourceInvalid(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_output_source", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.NoError(t, err)

	defer file.Close()

	_, err = io.WriteString(file, `
outputSource: none
`)
	assert.NoError(t, err)

	config, err := readFileToConfig(randomFile)
	assert.NoError(t, err)

	validErr := config.Validate()
	assert.ErrorContains(t, validErr, "invalid destination")
}

func TestConfig_Validate_ErrorTopicConfigInvalid(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_output_source_invalid_tc", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.NoError(t, err)

	defer file.Close()

	_, err = io.WriteString(file, `outputSource: snowflake`)
	assert.NoError(t, err)

	config, err := readFileToConfig(randomFile)
	assert.NoError(t, err)

	assert.ErrorContains(t, config.Validate(), "kafka config is nil")

	_, err = io.WriteString(file, `
kafka:
 bootstrapServer: kafka:9092
 groupID: 123
 username: foo
 password: bar
 topicConfigs:
  - { db: "", tableName: orders, schema: public, topic: orders, cdcFormat: debezium.mongodb}
  - { db: customer, tableName: customer, schema: public, topic: customer, cdcFormat: debezium.mongodb}
`)

	assert.NoError(t, err)

	config, err = readFileToConfig(randomFile)
	assert.NoError(t, err)
	assert.ErrorContains(t, config.Validate(), "failed to validate topic config")
}

func TestConfig_Validate_ErrorKafkaInvalid(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_output_source_invalid_tc", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.NoError(t, err)

	defer file.Close()

	_, err = io.WriteString(file, `outputSource: snowflake`)
	assert.NoError(t, err)

	config, err := readFileToConfig(randomFile)
	assert.NoError(t, err)
	assert.ErrorContains(t, config.Validate(), "kafka config is nil")

	_, err = io.WriteString(file, `
kafka:
 bootstrapServer:
 groupID: 123
 username: foo
 password: bar
 topicConfigs:
  - { db: customer, tableName: orders, schema: public, topic: orders, cdcFormat: debezium.mongodb, cdcKeyFormat: org.apache.kafka.connect.storage.StringConverter, dropDeletedColumns: true}
  - { db: customer, tableName: customer, schema: public, topic: customer, cdcFormat: debezium.mongodb, cdcKeyFormat: org.apache.kafka.connect.storage.StringConverter, softDelete: true}
  - { db: customer, tableName: customer55, schema: public, topic: customer55, cdcFormat: debezium.mongodb, cdcKeyFormat: org.apache.kafka.connect.storage.StringConverter, dropDeletedColumns: false, softDelete: true}
`)

	assert.NoError(t, err)

	config, err = readFileToConfig(randomFile)
	assert.NoError(t, err)

	assert.Equal(t, config.FlushIntervalSeconds, defaultFlushTimeSeconds)
	assert.Equal(t, int(config.BufferRows), defaultBufferPoolSize)

	assert.ErrorContains(t, config.Validate(), "kafka group or bootstrap server is empty")
	for _, tc := range config.Kafka.TopicConfigs {
		if tc.TableName == "orders" {
			assert.Equal(t, tc.DropDeletedColumns, true)
			assert.Equal(t, tc.SoftDelete, false)
		} else {
			assert.Equal(t, tc.DropDeletedColumns, false)
			assert.Equal(t, tc.SoftDelete, true)
		}
	}
}

func TestReadSentryDSNAndTelemetry(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_sentry_dsn", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.NoError(t, err)

	defer file.Close()

	_, err = io.WriteString(file,
		`
reporting:
 sentry:
  dsn: abc123
telemetry:
 metrics:
  provider: datadog
  settings:
   a: b
   foo: bar
   bar: true
   aNumber: 0.88
   tags:
    - env:bar
`)
	assert.NoError(t, err)

	config, err := readFileToConfig(randomFile)
	assert.NoError(t, err, "failed to read config file")
	assert.Equal(t, config.Reporting.Sentry.DSN, "abc123", config)
	assert.Equal(t, string(config.Telemetry.Metrics.Provider), "datadog")
	assert.Equal(t, config.Telemetry.Metrics.Settings, map[string]any{
		"a":       "b",
		"foo":     "bar",
		"bar":     true,
		"aNumber": 0.88,
		"tags":    []any{"env:bar"},
	})
}

func TestReadFileNotYAML(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), time.Now().String())
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.NoError(t, err)

	defer file.Close()

	_, err = io.WriteString(file, "foo foo")
	assert.NoError(t, err)

	_, err = readFileToConfig(randomFile)
	assert.ErrorContains(t, err, "yaml: unmarshal errors", "failed to read config file, because it's not proper yaml.")
}

func TestReadFileToConfig_Snowflake(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), time.Now().String())
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.NoError(t, err)

	defer file.Close()

	const (
		bootstrapServer = "server"
		groupID         = "group-id"
		username        = "user"
		password        = "dusty"

		snowflakeAccount  = "account"
		snowflakeUser     = "snowflake"
		snowflakePassword = "password"
		warehouse         = "warehouse"
		region            = "region"
		sentryDSN         = "sentry_url"
		application       = "foo"
	)

	_, err = io.WriteString(file, fmt.Sprintf(`
kafka:
 bootstrapServer: %s
 groupID: %s
 enableAWSMKSIAM: true
 username: %s
 password: %s
 topicConfigs:
  - { db: customer, tableName: orders, schema: public, skippedOperations: d}
  - { db: customer, tableName: customer, schema: public, skippedOperations: c}

snowflake:
 account: %s
 username: %s
 password: %s
 warehouse: %s
 region: %s
 application: %s
reporting:
 sentry:
  dsn: %s

`, bootstrapServer, groupID, username, password, snowflakeAccount,
		snowflakeUser, snowflakePassword, warehouse, region, application, sentryDSN))
	assert.NoError(t, err)

	config, err := readFileToConfig(randomFile)
	assert.NoError(t, err)

	assert.True(t, config.Kafka.EnableAWSMSKIAM)
	assert.Equal(t, username, config.Kafka.Username)
	assert.Equal(t, bootstrapServer, config.Kafka.BootstrapServer)
	assert.Equal(t, groupID, config.Kafka.GroupID)
	assert.Equal(t, password, config.Kafka.Password)

	orderIdx := -1
	customerIdx := -1
	for idx, topicConfig := range config.Kafka.TopicConfigs {
		assert.Equal(t, topicConfig.Database, "customer")
		assert.Equal(t, topicConfig.Schema, "public")

		if topicConfig.TableName == "orders" {
			orderIdx = idx
		}

		if topicConfig.TableName == "customer" {
			customerIdx = idx
		}
	}

	assert.True(t, customerIdx >= 0)
	assert.True(t, orderIdx >= 0)

	assert.Equal(t, "d", config.Kafka.TopicConfigs[orderIdx].SkippedOperations)
	assert.Equal(t, "c", config.Kafka.TopicConfigs[customerIdx].SkippedOperations)

	// Verify Snowflake config
	assert.Equal(t, snowflakeUser, config.Snowflake.Username)
	assert.Equal(t, snowflakePassword, config.Snowflake.Password)
	assert.Equal(t, snowflakeAccount, config.Snowflake.AccountID)
	assert.Equal(t, warehouse, config.Snowflake.Warehouse)
	assert.Equal(t, region, config.Snowflake.Region)
	assert.Equal(t, application, config.Snowflake.Application)
	assert.Equal(t, sentryDSN, config.Reporting.Sentry.DSN)
}

func TestReadFileToConfig_BigQuery(t *testing.T) {
	randomFile := fmt.Sprintf("/tmp/bq-%s", time.Now().String())
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.NoError(t, err)

	defer file.Close()

	const (
		bootstrapServer = "server"
		groupID         = "group-id"
		username        = "user"
		password        = "dusty"

		pathToCredentials = "/tmp/bq.json"
		dataset           = "dataset"
		projectID         = "artie"
	)

	_, err = io.WriteString(file, fmt.Sprintf(`
kafka:
 bootstrapServer: %s
 groupID: %s
 enableAWSMKSIAM: %v
 username: %s
 password: %s
 topicConfigs:
  - { db: customer, tableName: orders, schema: public}

bigquery:
 pathToCredentials: %s
 defaultDataset: %s
 projectID: %s
`, bootstrapServer, groupID, true, username, password, pathToCredentials, dataset, projectID))
	assert.NoError(t, err)

	// Now read it!
	config, err := readFileToConfig(randomFile)
	assert.NoError(t, err)
	assert.NotNil(t, config)

	// Verify BigQuery config
	assert.Equal(t, pathToCredentials, config.BigQuery.PathToCredentials)
	assert.Equal(t, dataset, config.BigQuery.DefaultDataset)
	assert.Equal(t, projectID, config.BigQuery.ProjectID)
}

func TestConfig_Validate(t *testing.T) {
	kafka := &kafkalib.Kafka{BootstrapServer: "foo", GroupID: "bar"}
	cfg := Config{
		Kafka:                kafka,
		FlushIntervalSeconds: 5,
		BufferRows:           500,
	}

	assert.ErrorContains(t, cfg.Validate(), "flush size pool has to be a positive number")
	cfg.Output = constants.Snowflake
	cfg.Queue = constants.Kafka
	cfg.FlushSizeKb = 5
	assert.ErrorContains(t, cfg.Validate(), "no topic configs")

	tc := kafkalib.TopicConfig{
		Database:     "db",
		TableName:    "table",
		Schema:       "schema",
		Topic:        "topic",
		CDCFormat:    constants.DBZPostgresAltFormat,
		CDCKeyFormat: "org.apache.kafka.connect.json.JsonConverter",
	}

	kafka.TopicConfigs = append(kafka.TopicConfigs, &tc)
	assert.NoError(t, cfg.Validate())

	tcs := cfg.TopicConfigs()
	assert.Len(t, tcs, 1)
	assert.Equal(t, tc, *tcs[0])

	// Check Snowflake and BigQuery for large rows
	// All should be fine.
	for _, destKind := range []constants.DestinationKind{constants.Snowflake, constants.BigQuery} {
		cfg.Output = destKind
		cfg.BufferRows = defaultBufferPoolSize + 1
		assert.NoError(t, cfg.Validate())
	}
	{
		// Invalid flush settings
		for i := range BufferPoolSizeMin {
			cfg.BufferRows = 500
			cfg.FlushIntervalSeconds = i
			assert.ErrorContains(t, cfg.Validate(), "flush interval is outside of our range")

			// Reset Flush
			cfg.FlushIntervalSeconds = 20
			cfg.BufferRows = uint(i)
			assert.ErrorContains(t, cfg.Validate(), "buffer pool is too small")
		}
	}

	cfg.BufferRows = 500
	cfg.FlushIntervalSeconds = 600
	assert.NoError(t, cfg.Validate())

	{
		// S3
		cfg.Output = constants.S3
		assert.ErrorContains(t, cfg.Validate(), "s3 settings are nil")
		cfg.S3 = &S3Settings{
			Bucket:             "foo",
			AwsSecretAccessKey: "foo",
			AwsAccessKeyID:     "bar",
			OutputFormat:       constants.ParquetFormat,
		}

		assert.NoError(t, cfg.Validate())
	}
	{
		// Now let's change to history mode and see.
		cfg.Mode = History
		kafka.TopicConfigs[0].DropDeletedColumns = true
		assert.ErrorContains(t, cfg.Validate(), "dropDeletedColumns is not supported in history mode")

		kafka.TopicConfigs[0].DropDeletedColumns = false
		kafka.TopicConfigs[0].IncludeDatabaseUpdatedAt = false
		assert.ErrorContains(t, cfg.Validate(), "includeDatabaseUpdatedAt is required in history mode")

		kafka.TopicConfigs[0].IncludeDatabaseUpdatedAt = true
		assert.NoError(t, cfg.Validate())
	}

	for _, num := range []int{-500, -300, -5, 0} {
		cfg.FlushSizeKb = num
		assert.ErrorContains(t, cfg.Validate(), "flush size pool has to be a positive number")
	}
}
