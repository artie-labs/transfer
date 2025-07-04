package artie

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

const keyString = "Struct{id=12}"

func TestNewMessage(t *testing.T) {
	kafkaMsg := kafka.Message{
		Topic:     "test_topic",
		Partition: 5,
		Key:       []byte(keyString),
		Value:     []byte("kafka_value"),
	}

	msg := NewMessage(kafkaMsg)
	assert.Equal(t, "test_topic", msg.Topic())
	assert.Equal(t, 5, msg.Partition())
	assert.Equal(t, keyString, string(msg.Key()))
	assert.Equal(t, "kafka_value", string(msg.Value()))
}
