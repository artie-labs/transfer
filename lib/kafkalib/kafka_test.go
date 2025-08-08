package kafkalib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKafka_String(t *testing.T) {
	k := Kafka{
		BootstrapServer: "server",
		GroupID:         "group-id",
		Username:        "",
		Password:        "",
	}

	assert.Equal(t, "bootstrapServer=server, groupID=group-id, user_set=false, pass_set=false", k.String())

	k.Username = "foo"
	assert.Equal(t, "bootstrapServer=server, groupID=group-id, user_set=true, pass_set=false", k.String())

	k.Password = "bar"
	assert.Equal(t, "bootstrapServer=server, groupID=group-id, user_set=true, pass_set=true", k.String())
}
func TestCfg_KafkaBootstrapServers(t *testing.T) {
	{
		// Single broker
		kafka := Kafka{BootstrapServer: "localhost:9092"}
		assert.Equal(t, []string{"localhost:9092"}, kafka.BootstrapServers(false))
	}
	{
		// Multiple brokers
		kafkaWithMultipleBrokers := Kafka{BootstrapServer: "a:9092,b:9093,c:9094"}
		assert.Equal(t, []string{"a:9092", "b:9093", "c:9094"}, kafkaWithMultipleBrokers.BootstrapServers(false))
	}
	{
		// Randomize
		kafkaWithMultipleBrokers := Kafka{BootstrapServer: "a:9092,b:9093,c:9094"}
		assert.ElementsMatch(t, []string{"a:9092", "b:9093", "c:9094"}, kafkaWithMultipleBrokers.BootstrapServers(true))
	}
}
