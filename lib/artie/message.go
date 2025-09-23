package artie

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

type MessageType interface {
	kafka.Message
}

type Message[M MessageType] interface {
	GetMessage() M
	EmitRowLag(metricsClient base.Client, mode config.Mode, groupID, table string)
	EmitIngestionLag(metricsClient base.Client, mode config.Mode, groupID, table string)
	PublishTime() time.Time
	Topic() string
	Partition() int
	Offset() int64
	Key() []byte
	Value() []byte
}

func NewMessage[M MessageType](msg M) (Message[M], error) {
	switch m := any(msg).(type) {
	case kafka.Message:
		return any(KafkaGoMessage{message: m}).(Message[M]), nil
	default:
		return nil, fmt.Errorf("unsupported message type")
	}
}

func BuildLogFields[M MessageType](msg M) ([]any, error) {
	switch m := any(msg).(type) {
	case kafka.Message:
		return []any{
			slog.String("topic", m.Topic),
			slog.Int64("offset", m.Offset),
			slog.String("key", string(m.Key)),
			slog.String("value", string(m.Value)),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported message type %v", m)
	}
}

type KafkaGoMessage struct {
	message kafka.Message
}

func (m KafkaGoMessage) GetMessage() kafka.Message {
	return m.message
}

// EmitRowLag will diff against the partition's high watermark and the message's offset
func (m KafkaGoMessage) EmitRowLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
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

func (m KafkaGoMessage) EmitIngestionLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.Timing("ingestion.lag", time.Since(m.PublishTime()), map[string]string{
		"mode":    mode.String(),
		"groupID": groupID,
		"table":   table,
	})
}

func (m KafkaGoMessage) PublishTime() time.Time {
	return m.message.Time
}

func (m KafkaGoMessage) Topic() string {
	return m.message.Topic
}

func (m KafkaGoMessage) Partition() int {
	return m.message.Partition
}

func (m KafkaGoMessage) Offset() int64 {
	return m.message.Offset
}

func (m KafkaGoMessage) Key() []byte {
	return m.message.Key
}

func (m KafkaGoMessage) Value() []byte {
	return m.message.Value
}
