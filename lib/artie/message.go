package artie

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/segmentio/kafka-go"
)

type Kind int

const (
	Invalid Kind = iota
	Kafka
	PubSub
)

type pubsubWrapper struct {
	topic string
	*pubsub.Message
}

type Message struct {
	KafkaMsg *kafka.Message
	PubSub   *pubsubWrapper
}

func NewMessage(kafkaMsg *kafka.Message, pubsubMsg *pubsub.Message, topic string) Message {
	var msg Message
	if kafkaMsg != nil {
		msg.KafkaMsg = kafkaMsg
	}

	if pubsubMsg != nil {
		msg.PubSub = &pubsubWrapper{
			topic:   topic,
			Message: pubsubMsg,
		}
	}

	return msg
}

func (m *Message) Kind() Kind {
	if m.KafkaMsg != nil {
		return Kafka
	}

	if m.PubSub != nil {
		return PubSub
	}

	return Invalid
}

// EmitRowLag will diff against the partition's high watermark and the message's offset
func (m *Message) EmitRowLag(ctx context.Context, groupID, table string) {
	if m.KafkaMsg == nil {
		return
	}

	metrics.FromContext(ctx).Gauge("row.lag", float64(m.KafkaMsg.HighWaterMark-m.KafkaMsg.Offset), map[string]string{
		"groupID":   groupID,
		"topic":     m.Topic(),
		"table":     table,
		"partition": m.Partition(),
	})
}

func (m *Message) EmitIngestionLag(ctx context.Context, groupID, table string) {
	metrics.FromContext(ctx).Timing("ingestion.lag", time.Since(m.PublishTime()), map[string]string{
		"groupID":   groupID,
		"topic":     m.Topic(),
		"table":     table,
		"partition": m.Partition(),
	})
}

func (m *Message) PublishTime() time.Time {
	if m.KafkaMsg != nil {
		return m.KafkaMsg.Time
	}

	if m.PubSub != nil {
		return m.PubSub.PublishTime
	}

	return time.Time{}
}

func (m *Message) Topic() string {
	if m.KafkaMsg != nil {
		return m.KafkaMsg.Topic
	}

	if m.PubSub != nil {
		return m.PubSub.topic
	}

	return ""
}

func (m *Message) Partition() string {
	if m.KafkaMsg != nil {
		return fmt.Sprint(m.KafkaMsg.Partition)
	}

	if m.PubSub != nil {
		// Pub/Sub doesn't have partitions.
		return "no_partition"
	}

	return ""
}

func (m *Message) Key() []byte {
	if m.KafkaMsg != nil {
		return m.KafkaMsg.Key
	}

	if m.PubSub != nil {
		return []byte(m.PubSub.OrderingKey)
	}

	return nil
}

func (m *Message) Value() []byte {
	if m.KafkaMsg != nil {
		return m.KafkaMsg.Value
	}

	if m.PubSub != nil {
		return m.PubSub.Data
	}

	return nil
}
