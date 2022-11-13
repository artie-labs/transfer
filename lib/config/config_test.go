package config

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadNonExistentFile(t *testing.T) {
	config, err := readFileToConfig("/tmp/213213231312")
	assert.Error(t, err)
	assert.Nil(t, config)
}

func TestReadSentryDSN(t *testing.T) {
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
`)
	assert.Nil(t, err)

	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err, "failed to read config file")
	assert.Equal(t, config.Reporting.Sentry.DSN, "abc123", config)
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

`, bootstrapServer, groupID, username, password, snowflakeAccount,
		snowflakeUser, snowflakePassword, warehouse, region, sentryDSN))
	assert.Nil(t, err)

	// Now read it!
	config, err := readFileToConfig(randomFile)
	assert.Nil(t, err)
	assert.NotNil(t, config)

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
