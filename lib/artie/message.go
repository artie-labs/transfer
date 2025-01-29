package artie

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

type Kind int

const (
	Invalid Kind = iota
	Kafka
)

type Message struct {
	KafkaMsg *kafka.Message
}

func KafkaMsgLogFields(msg kafka.Message) []any {
	return []any{
		slog.String("topic", msg.Topic),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)),
		slog.String("value", string(msg.Value)),
	}
}

func NewMessage(kafkaMsg *kafka.Message, topic string) Message {
	var msg Message
	if kafkaMsg != nil {
		msg.KafkaMsg = kafkaMsg
	}

	return msg
}

func (m *Message) Kind() Kind {
	if m.KafkaMsg != nil {
		return Kafka
	}

	return Invalid
}

// EmitRowLag will diff against the partition's high watermark and the message's offset
// This function is only available for Kafka since Kafka has the concept of offsets and watermarks.
func (m *Message) EmitRowLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	if m.KafkaMsg == nil {
		return
	}

	metricsClient.GaugeWithSample(
		"row.lag",
		float64(m.KafkaMsg.HighWaterMark-m.KafkaMsg.Offset),
		map[string]string{
			"mode":    mode.String(),
			"groupID": groupID,
			"table":   table,
		},
		0.5)
}

func (m *Message) EmitIngestionLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.Timing("ingestion.lag", time.Since(m.PublishTime()), map[string]string{
		"mode":    mode.String(),
		"groupID": groupID,
		"table":   table,
	})
}

func (m *Message) PublishTime() time.Time {
	if m.KafkaMsg != nil {
		return m.KafkaMsg.Time
	}

	return time.Time{}
}

func (m *Message) Topic() string {
	if m.KafkaMsg != nil {
		return m.KafkaMsg.Topic
	}

	return ""
}

func (m *Message) Partition() string {
	if m.KafkaMsg != nil {
		return fmt.Sprint(m.KafkaMsg.Partition)
	}

	return ""
}

func (m *Message) Key() []byte {
	if m.KafkaMsg != nil {
		return m.KafkaMsg.Key
	}

	return nil
}

func (m *Message) Value() []byte {
	if m.KafkaMsg != nil {
		return m.KafkaMsg.Value
	}

	return nil
}
