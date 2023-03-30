package config

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"io"
	"os"
	"strings"
	"testing"
	"time"

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
%s
`, validKafkaTopic))
	assert.Nil(t, err)

	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)

	assert.Nil(t, config.Validate())
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

func TestReadFileToConfig(t *testing.T) {
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

	// Verify Sno3wflake config
	assert.Equal(t, config.Snowflake.Username, snowflakeUser)
	assert.Equal(t, config.Snowflake.Password, snowflakePassword)
	assert.Equal(t, config.Snowflake.AccountID, snowflakeAccount)
	assert.Equal(t, config.Snowflake.Warehouse, warehouse)
	assert.Equal(t, config.Snowflake.Region, region)
	assert.Equal(t, config.Reporting.Sentry.DSN, sentryDSN)
}

func TestConfig_Validate(t *testing.T) {
	pubsub := &Pubsub{}
	cfg := &Config{
		Pubsub: pubsub,
	}

	assert.Contains(t, cfg.Validate().Error(), "is invalid")

	cfg.Output = constants.Snowflake
	cfg.Queue = constants.PubSub
	assert.Contains(t, cfg.Validate().Error(), "no pubsub topic configs")

	pubsub.TopicConfigs = []*kafkalib.TopicConfig{
		{
			Database:  "db",
			TableName: "table",
			Schema:    "schema",
			Topic:     "topic",
		},
	}

	assert.Contains(t, cfg.Validate().Error(), "topic config is invalid")
	pubsub.TopicConfigs[0].CDCFormat = constants.DBZPostgresAltFormat
	pubsub.TopicConfigs[0].CDCKeyFormat = "org.apache.kafka.connect.json.JsonConverter"
	assert.Contains(t, cfg.Validate().Error(), "pubsub settings is invalid")

	pubsub.ProjectID = "project_id"
	pubsub.PathToCredentials = "/tmp/abc"
	assert.Nil(t, cfg.Validate())
}
