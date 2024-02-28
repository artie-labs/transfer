package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"gopkg.in/yaml.v3"

	"github.com/stretchr/testify/assert"
)

func TestSharedTransferConfig(t *testing.T) {
	{
		var sharedTransferCfg SharedTransferConfig
		validBody := `
typingSettings:
 additionalDateFormats: ["yyyy-MM-dd1"]
 createAllColumnsIfAvailable: true
`
		err := yaml.Unmarshal([]byte(validBody), &sharedTransferCfg)
		assert.NoError(t, err)

		assert.True(t, sharedTransferCfg.TypingSettings.CreateAllColumnsIfAvailable)
		assert.Equal(t, "yyyy-MM-dd1", sharedTransferCfg.TypingSettings.AdditionalDateFormats[0])
	}
}

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

func TestKafka_BootstrapServers(t *testing.T) {
	type _tc struct {
		bootstrapServerString    string
		expectedBootstrapServers []string
	}

	tcs := []_tc{
		{
			bootstrapServerString:    "localhost:9092",
			expectedBootstrapServers: []string{"localhost:9092"},
		},
		{
			bootstrapServerString:    "a:9092,b:9093,c:9094",
			expectedBootstrapServers: []string{"a:9092", "b:9093", "c:9094"},
		},
	}

	for idx, tc := range tcs {
		k := Kafka{
			BootstrapServer: tc.bootstrapServerString,
		}

		assert.Equal(t, tc.expectedBootstrapServers, k.BootstrapServers(), idx)
	}
}

func TestKafka_String(t *testing.T) {
	k := Kafka{
		BootstrapServer: "server",
		GroupID:         "group-id",
		Username:        "",
		Password:        "",
	}

	assert.Contains(t, k.String(), k.BootstrapServer, k.String())
	assert.Contains(t, k.String(), k.GroupID, k.String())
	assert.Contains(t, k.String(), "pass_set=false", k.String())
	assert.Contains(t, k.String(), "user_set=false", k.String())

	k.Username = "foo"
	assert.Contains(t, k.String(), "user_set=true", k.String())
	assert.Contains(t, k.String(), "pass_set=false", k.String())

	k.Password = "bar"
	assert.Contains(t, k.String(), "user_set=true", k.String())
	assert.Contains(t, k.String(), "pass_set=true", k.String())
}

func TestReadNonExistentFile(t *testing.T) {
	config, err := readFileToConfig(filepath.Join(t.TempDir(), "213213231312"))
	assert.ErrorContains(t, err, "no such file or directory")
	assert.Nil(t, config)
}

func TestOutputSourceValid(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_output_source_valid", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.Nil(t, err)

	defer file.Close()

	_, err = io.WriteString(file, fmt.Sprintf(
		`
outputSource: snowflake
flushIntervalSeconds: 15
bufferRows: 10
%s
`, validKafkaTopic))
	assert.Nil(t, err)

	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)

	assert.Equal(t, config.FlushIntervalSeconds, 15)
	assert.Equal(t, int(config.BufferRows), 10)

	tcs, err := config.TopicConfigs()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tcs))
	for _, tc := range tcs {
		tc.Load()
		assert.Equal(t, "customer", tc.Database)
	}

	assert.Nil(t, config.Validate())
}

func TestOutputSourceInvalid(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_output_source", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.Nil(t, err)

	defer file.Close()

	_, err = io.WriteString(file,
		`
outputSource: none
`)
	assert.Nil(t, err)

	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)

	validErr := config.Validate()
	assert.Error(t, validErr)
	assert.ErrorContains(t, validErr, "is invalid", validErr.Error())
}

func TestConfig_Validate_ErrorTopicConfigInvalid(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_output_source_invalid_tc", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.Nil(t, err)

	defer file.Close()

	_, err = io.WriteString(file,
		`
outputSource: test
`)
	assert.Nil(t, err)

	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)

	validErr := config.Validate()
	assert.Error(t, validErr)
	assert.True(t, strings.Contains(validErr.Error(), "no kafka"), validErr.Error())

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

	assert.Nil(t, err)

	config, err = readFileToConfig(randomFile)
	assert.Nil(t, err)

	validErr = config.Validate()
	assert.Error(t, validErr)
	assert.ErrorContains(t, validErr, "config is invalid, topic config is invalid")
}

func TestConfig_Validate_ErrorKafkaInvalid(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), fmt.Sprintf("%s_output_source_invalid_tc", time.Now().String()))
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.Nil(t, err)

	defer file.Close()

	_, err = io.WriteString(file,
		`
outputSource: test
`)
	assert.Nil(t, err)

	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)

	validErr := config.Validate()
	assert.Error(t, validErr)
	assert.True(t, strings.Contains(validErr.Error(), "no kafka"), validErr.Error())

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

	assert.Nil(t, err)

	config, err = readFileToConfig(randomFile)
	assert.Nil(t, err)

	assert.Equal(t, config.FlushIntervalSeconds, defaultFlushTimeSeconds)
	assert.Equal(t, int(config.BufferRows), bufferPoolSizeEnd)

	tcs, err := config.TopicConfigs()
	assert.NoError(t, err)
	for _, tc := range tcs {
		tc.Load()
	}

	assert.ErrorContains(t, config.Validate(), "kafka settings is invalid")
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
	assert.Nil(t, err)

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
	assert.Nil(t, err)

	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err, "failed to read config file")
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
	assert.Nil(t, err)

	defer file.Close()

	_, err = io.WriteString(file, "foo foo")
	assert.Nil(t, err)

	config, err := readFileToConfig(randomFile)
	assert.Nil(t, config)
	assert.Error(t, err, "failed to read config file, because it's not proper yaml.")
}

func TestReadFileToConfig_Snowflake(t *testing.T) {
	randomFile := filepath.Join(t.TempDir(), time.Now().String())
	defer os.Remove(randomFile)

	file, err := os.Create(randomFile)
	assert.Nil(t, err)

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

	_, err = io.WriteString(file, fmt.Sprintf(
		`
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

sharedTransferConfig:
  typingSettings:
    createAllColumnsIfAvailable: true


reporting:
 sentry:
  dsn: %s

`, bootstrapServer, groupID, username, password, snowflakeAccount,
		snowflakeUser, snowflakePassword, warehouse, region, application, sentryDSN))
	assert.Nil(t, err)

	// Now read it!
	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)
	assert.NotNil(t, config)

	assert.True(t, config.Kafka.EnableAWSMSKIAM)
	assert.Equal(t, username, config.Kafka.Username)
	assert.Equal(t, bootstrapServer, config.Kafka.BootstrapServer)
	assert.Equal(t, groupID, config.Kafka.GroupID)
	assert.Equal(t, password, config.Kafka.Password)
	assert.True(t, config.SharedTransferConfig.TypingSettings.CreateAllColumnsIfAvailable)

	orderIdx := -1
	customerIdx := -1
	for idx, topicConfig := range config.Kafka.TopicConfigs {
		topicConfig.Load()

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

	assert.True(t, config.Kafka.TopicConfigs[orderIdx].ShouldSkip("d"))
	assert.True(t, config.Kafka.TopicConfigs[customerIdx].ShouldSkip("c"))

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
	assert.Nil(t, err)

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

	_, err = io.WriteString(file, fmt.Sprintf(
		`
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
	assert.Nil(t, err)

	// Now read it!
	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)
	assert.NotNil(t, config)

	// Verify BigQuery config
	assert.Equal(t, pathToCredentials, config.BigQuery.PathToCredentials)
	assert.Equal(t, dataset, config.BigQuery.DefaultDataset)
	assert.Equal(t, projectID, config.BigQuery.ProjectID)
}

func TestConfig_Validate(t *testing.T) {
	pubsub := &Pubsub{
		ProjectID:         "foo",
		PathToCredentials: "bar",
	}
	cfg := &Config{
		Pubsub:               pubsub,
		FlushIntervalSeconds: 5,
		BufferRows:           500,
	}

	assert.ErrorContains(t, cfg.Validate(), "is invalid")

	cfg.Output = constants.Snowflake
	cfg.Queue = constants.PubSub
	cfg.FlushSizeKb = 5
	assert.ErrorContains(t, cfg.Validate(), "no topic configs")

	tc := kafkalib.TopicConfig{
		Database:  "db",
		TableName: "table",
		Schema:    "schema",
		Topic:     "topic",
	}

	tc.Load()

	pubsub.TopicConfigs = []*kafkalib.TopicConfig{&tc}
	pubsub.ProjectID = ""
	assert.ErrorContains(t, cfg.Validate(), "pubsub settings is invalid")
	pubsub.ProjectID = "foo"

	assert.ErrorContains(t, cfg.Validate(), "topic config is invalid")
	pubsub.TopicConfigs[0].CDCFormat = constants.DBZPostgresAltFormat
	pubsub.TopicConfigs[0].CDCKeyFormat = "org.apache.kafka.connect.json.JsonConverter"

	pubsub.ProjectID = "project_id"
	pubsub.PathToCredentials = "/tmp/abc"
	assert.Nil(t, cfg.Validate())

	tcs, err := cfg.TopicConfigs()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tcs))
	assert.Equal(t, tc, *tcs[0])

	// Check Snowflake and BigQuery for large rows
	// All should be fine.
	for _, destKind := range []constants.DestinationKind{constants.Snowflake, constants.BigQuery} {
		cfg.Output = destKind
		cfg.BufferRows = bufferPoolSizeEnd + 1
		assert.Nil(t, cfg.Validate())
	}

	// Test the various flush error settings.
	for i := 0; i < bufferPoolSizeStart; i++ {
		// Reset buffer rows.
		cfg.BufferRows = 500
		cfg.FlushIntervalSeconds = i
		assert.ErrorContains(t, cfg.Validate(), "flush interval is outside of our range")

		// Reset Flush
		cfg.FlushIntervalSeconds = 20
		cfg.BufferRows = uint(i)
		assert.ErrorContains(t, cfg.Validate(), "buffer pool is too small")
	}

	cfg.BufferRows = 500
	cfg.FlushIntervalSeconds = 600
	assert.Nil(t, cfg.Validate())

	// Now that we have a valid output, let's test with S3.
	cfg.Output = constants.S3
	assert.Error(t, cfg.Validate())
	cfg.S3 = &S3Settings{
		Bucket:             "foo",
		AwsSecretAccessKey: "foo",
		AwsAccessKeyID:     "bar",
		OutputFormat:       constants.ParquetFormat,
	}

	assert.Nil(t, cfg.Validate())

	// Now let's change to history mode and see.
	cfg.Mode = History
	pubsub.TopicConfigs[0].DropDeletedColumns = true
	assert.ErrorContains(t, cfg.Validate(), "config is invalid, drop deleted columns is not supported in history mode")

	pubsub.TopicConfigs[0].DropDeletedColumns = false
	pubsub.TopicConfigs[0].IncludeDatabaseUpdatedAt = false
	assert.ErrorContains(t, cfg.Validate(), "config is invalid, include database updated at is required in history mode")

	pubsub.TopicConfigs[0].IncludeDatabaseUpdatedAt = true
	assert.NoError(t, cfg.Validate())
	// End history mode

	for _, num := range []int{-500, -300, -5, 0} {
		cfg.FlushSizeKb = num
		assert.ErrorContains(t, cfg.Validate(), "config is invalid, flush size pool has to be a positive number")
	}
}

func TestCfg_KafkaBootstrapServers(t *testing.T) {
	kafka := Kafka{
		BootstrapServer: "localhost:9092",
	}

	assert.Equal(t, []string{"localhost:9092"}, strings.Split(kafka.BootstrapServer, ","))

	kafkaWithMultipleBrokers := Kafka{
		BootstrapServer: "a:9092,b:9093,c:9094",
	}

	var brokers []string
	brokers = append(brokers, strings.Split(kafkaWithMultipleBrokers.BootstrapServer, ",")...)

	assert.Equal(t, []string{"a:9092", "b:9093", "c:9094"}, brokers)
}
