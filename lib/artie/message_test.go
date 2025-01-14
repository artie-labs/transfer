package artie

import (
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

const keyString = "Struct{id=12}"

func TestNewMessage_Pubsub(t *testing.T) {
	msg := NewMessage(nil, nil, "")
	assert.Equal(t, msg.Topic(), "", "empty topic")

	pubsubMsg := &pubsub.Message{}
	msg = NewMessage(nil, pubsubMsg, "")
	assert.Equal(t, "no_partition", msg.Partition())

	pubsubMsg.Data = []byte("hello_world")
	assert.Equal(t, "hello_world", string(msg.Value()))

	pubsubMsg.OrderingKey = keyString
	assert.Equal(t, []byte(keyString), msg.Key())

	msg = NewMessage(nil, pubsubMsg, "database.schema.table")
	assert.Equal(t, "database.schema.table", msg.Topic())
}

func TestNewMessage(t *testing.T) {
	kafkaMsg := &kafka.Message{
		Topic:     "test_topic",
		Partition: 5,
		Key:       []byte(keyString),
		Value:     []byte("kafka_value"),
	}

	msg := NewMessage(kafkaMsg, nil, "")
	assert.Equal(t, "test_topic", msg.Topic())
	assert.Equal(t, "5", msg.Partition())
	assert.Equal(t, keyString, string(msg.Key()))
	assert.Equal(t, "kafka_value", string(msg.Value()))
}
