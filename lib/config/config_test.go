package config

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
`, bootstrapServer, groupID, username, password, snowflakeAccount,
		snowflakeUser, snowflakePassword, warehouse, region))
	assert.Nil(t, err)

	// Now read it!
	config, err := ReadFileToConfig(randomFile)
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
}
