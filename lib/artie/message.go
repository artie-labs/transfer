package artie

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

type Message struct {
	message kafka.Message
}

func (m Message) GetMessage() kafka.Message {
	return m.message
}

func BuildLogFields(msg kafka.Message) []any {
	return []any{
		slog.String("topic", msg.Topic),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)),
		slog.String("value", string(msg.Value)),
	}
}

func NewMessage(msg kafka.Message) Message {
	return Message{message: msg}
}

// EmitRowLag will diff against the partition's high watermark and the message's offset
func (m Message) EmitRowLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.GaugeWithSample(
		"row.lag",
		float64(m.message.HighWaterMark-m.message.Offset),
		map[string]string{
			"mode":    mode.String(),
			"groupID": groupID,
			"table":   table,
		},
		0.5)
}

func (m Message) EmitIngestionLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.Timing("ingestion.lag", time.Since(m.PublishTime()), map[string]string{
		"mode":    mode.String(),
		"groupID": groupID,
		"table":   table,
	})
}

func (m Message) PublishTime() time.Time {
	return m.message.Time
}

func (m Message) Topic() string {
	return m.message.Topic
}

func (m Message) Partition() int {
	return m.message.Partition
}

func (m Message) Key() []byte {
	return m.message.Key
}

func (m Message) Value() []byte {
	return m.message.Value
}

func (m Message) Offset() int64 {
	return m.message.Offset
}

func (m Message) KafkaInfo() string {
	return fmt.Sprintf("Topic: %q, Partition: %d, Offset: %d", m.message.Topic, m.message.Partition, m.message.Offset)
}
