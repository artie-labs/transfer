package artie

import (
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
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

func KafkaMsgLogFields(msg kafka.Message) []any {
	return []any{
		slog.String("topic", msg.Topic),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)),
		slog.String("value", string(msg.Value)),
	}
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
// This function is only available for Kafka since Kafka has the concept of offsets and watermarks.
func (m *Message) EmitRowLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	if m.KafkaMsg == nil {
		return
	}

	metricsClient.GaugeWithSample("row.lag", float64(m.KafkaMsg.HighWaterMark-m.KafkaMsg.Offset), map[string]string{
		"mode":      mode.String(),
		"groupID":   groupID,
		"topic":     m.Topic(),
		"table":     table,
		"partition": m.Partition(),
	}, 0.5)
}

func (m *Message) EmitIngestionLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.Timing("ingestion.lag", time.Since(m.PublishTime()), map[string]string{
		"mode":      mode.String(),
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
