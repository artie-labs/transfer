package artie

import (
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

func (m Message) LogFields() []any {
	return []any{
		slog.String("topic", m.message.Topic),
		slog.Int64("offset", m.message.Offset),
		slog.String("key", string(m.message.Key)),
		slog.String("value", string(m.message.Value)),
	}
}

func NewMessage(msg kafka.Message) Message {
	return Message{message: msg}
}

// EmitRowLag will diff against the partition's high watermark and the message's offset
// This function is only available for Kafka since Kafka has the concept of offsets and watermarks.
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
