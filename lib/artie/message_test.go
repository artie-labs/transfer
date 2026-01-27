package artie

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

const keyString = "Struct{id=12}"

var now = time.Now()

func TestNewFranzGoMessage(t *testing.T) {
	kafkaMsg := kgo.Record{
		Topic:     "test_topic",
		Partition: 5,
		Offset:    100,
		Key:       []byte(keyString),
		Value:     []byte("kafka_value"),
		Timestamp: now,
	}

	msg := NewFranzGoMessage(kafkaMsg, 1000)
	assert.Equal(t, now, msg.PublishTime())
	assert.Equal(t, "test_topic", msg.Topic())
	assert.Equal(t, 5, msg.Partition())
	assert.Equal(t, int64(100), msg.Offset())
	assert.Equal(t, keyString, string(msg.Key()))
	assert.Equal(t, "kafka_value", string(msg.Value()))
	assert.Equal(t, int64(1000), msg.HighWaterMark())
}
