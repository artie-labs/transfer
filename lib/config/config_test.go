package config

import (
	"fmt"
	"io"
	"os"
	"strings"
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
  - { db: customer, tableName: orders, schema: public, topic: orders, cdcFormat: debezium.mongodb}
  - { db: customer, tableName: customer, schema: public, topic: customer, cdcFormat: debezium.mongodb}
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
	config, err := readFileToConfig("/tmp/213213231312")
	assert.Error(t, err)
	assert.Nil(t, config)
}

func TestOutputSourceValid(t *testing.T) {
	randomFile := fmt.Sprintf("/tmp/%s_output_source_valid", time.Now().String())
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

	assert.Nil(t, config.Validate())

	tcs, err := config.TopicConfigs()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tcs))
	for _, tc := range tcs {
		assert.Equal(t, "customer", tc.Database)
	}
}

func TestOutputSourceInvalid(t *testing.T) {
	randomFile := fmt.Sprintf("/tmp/%s_output_source", time.Now().String())
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
	assert.True(t, strings.Contains(validErr.Error(), "is invalid"), validErr.Error())
}

func TestConfig_Validate_ErrorTopicConfigInvalid(t *testing.T) {
	randomFile := fmt.Sprintf("/tmp/%s_output_source_invalid_tc", time.Now().String())
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
	assert.True(t, strings.Contains(validErr.Error(), "topic config is invalid"), validErr.Error())
}

func TestConfig_Validate_ErrorKafkaInvalid(t *testing.T) {
	randomFile := fmt.Sprintf("/tmp/%s_output_source_invalid_tc", time.Now().String())
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
  - { db: customer, tableName: orders, schema: public, topic: orders, cdcFormat: debezium.mongodb, dropDeletedColumns: true}
  - { db: customer, tableName: customer, schema: public, topic: customer, cdcFormat: debezium.mongodb, softDelete: true}
  - { db: customer, tableName: customer55, schema: public, topic: customer55, cdcFormat: debezium.mongodb, dropDeletedColumns: false, softDelete: true}
`)

	assert.Nil(t, err)

	config, err = readFileToConfig(randomFile)
	assert.Nil(t, err)

	assert.Equal(t, config.FlushIntervalSeconds, defaultFlushTimeSeconds)
	assert.Equal(t, int(config.BufferRows), bufferPoolSizeEnd)

	validErr = config.Validate()
	assert.Error(t, validErr)
	assert.True(t, strings.Contains(validErr.Error(), "kafka settings is invalid"), validErr.Error())

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
	randomFile := fmt.Sprintf("/tmp/%s_sentry_dsn", time.Now().String())
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
	assert.Equal(t, config.Telemetry.Metrics.Settings, map[string]interface{}{
		"a":       "b",
		"foo":     "bar",
		"bar":     true,
		"aNumber": 0.88,
		"tags":    []interface{}{"env:bar"},
	})
}

func TestReadFileNotYAML(t *testing.T) {
	randomFile := fmt.Sprintf("/tmp/%s", time.Now().String())
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
	randomFile := fmt.Sprintf("/tmp/%s", time.Now().String())
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
  - { db: customer, tableName: customer, schema: public}

snowflake:
 account: %s
 username: %s
 password: %s
 warehouse: %s
 region: %s

reporting:
 sentry:
  dsn: %s

`, bootstrapServer, groupID, true, username, password, snowflakeAccount,
		snowflakeUser, snowflakePassword, warehouse, region, sentryDSN))
	assert.Nil(t, err)

	// Now read it!
	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)
	assert.NotNil(t, config)

	assert.Equal(t, config.Kafka.EnableAWSMSKIAM, true)
	assert.Equal(t, config.Kafka.Username, username)
	assert.Equal(t, config.Kafka.BootstrapServer, bootstrapServer)
	assert.Equal(t, config.Kafka.GroupID, groupID)
	assert.Equal(t, config.Kafka.Password, password)

	var foundOrder bool
	var foundCustomer bool

	for _, topicConfig := range config.Kafka.TopicConfigs {
		assert.Equal(t, topicConfig.Database, "customer")
		assert.Equal(t, topicConfig.Schema, "public")

		if topicConfig.TableName == "orders" {
			foundOrder = true
		}

		if topicConfig.TableName == "customer" {
			foundCustomer = true
		}
	}

	assert.True(t, foundCustomer)
	assert.True(t, foundOrder)

	// Verify Snowflake config
	assert.Equal(t, config.Snowflake.Username, snowflakeUser)
	assert.Equal(t, config.Snowflake.Password, snowflakePassword)
	assert.Equal(t, config.Snowflake.AccountID, snowflakeAccount)
	assert.Equal(t, config.Snowflake.Warehouse, warehouse)
	assert.Equal(t, config.Snowflake.Region, region)
	assert.Equal(t, config.Reporting.Sentry.DSN, sentryDSN)
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
	assert.Equal(t, config.BigQuery.PathToCredentials, pathToCredentials)
	assert.Equal(t, config.BigQuery.DefaultDataset, dataset)
	assert.Equal(t, config.BigQuery.ProjectID, projectID)
}

func TestConfig_Validate(t *testing.T) {
	pubsub := &Pubsub{}
	cfg := &Config{
		Pubsub:               pubsub,
		FlushIntervalSeconds: 5,
		BufferRows:           500,
	}

	assert.Contains(t, cfg.Validate().Error(), "is invalid")

	cfg.Output = constants.Snowflake
	cfg.Queue = constants.PubSub
	cfg.FlushSizeKb = 5
	assert.Contains(t, cfg.Validate().Error(), "no pubsub topic configs")

	tc := kafkalib.TopicConfig{
		Database:  "db",
		TableName: "table",
		Schema:    "schema",
		Topic:     "topic",
	}

	pubsub.TopicConfigs = []*kafkalib.TopicConfig{&tc}

	assert.Contains(t, cfg.Validate().Error(), "topic config is invalid")
	pubsub.TopicConfigs[0].CDCFormat = constants.DBZPostgresAltFormat
	pubsub.TopicConfigs[0].CDCKeyFormat = "org.apache.kafka.connect.json.JsonConverter"
	assert.Contains(t, cfg.Validate().Error(), "pubsub settings is invalid")

	pubsub.ProjectID = "project_id"
	pubsub.PathToCredentials = "/tmp/abc"
	assert.Nil(t, cfg.Validate())

	tcs, err := cfg.TopicConfigs()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tcs))
	assert.Equal(t, tc, *tcs[0])

	// Check Snowflake and BigQuery for large rows
	// All should be fine.
	for _, destKind := range []constants.DestinationKind{constants.SnowflakeStages, constants.Snowflake, constants.BigQuery} {
		cfg.Output = destKind
		cfg.BufferRows = bufferPoolSizeEnd + 1
		assert.Nil(t, cfg.Validate())
	}

	// Test the various flush error settings.
	for i := 0; i < bufferPoolSizeStart; i++ {
		// Reset buffer rows.
		cfg.BufferRows = 500
		cfg.FlushIntervalSeconds = i
		assert.Contains(t, cfg.Validate().Error(), "flush interval is outside of our range")

		// Reset Flush
		cfg.FlushIntervalSeconds = 20
		cfg.BufferRows = uint(i)
		assert.Contains(t, cfg.Validate().Error(), "buffer pool is too small")
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

	for _, num := range []int{-500, -300, -5, 0} {
		cfg.FlushSizeKb = num
		assert.Contains(t, cfg.Validate().Error(), "config is invalid, flush size pool has to be a positive number")
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
