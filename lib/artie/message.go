package artie

import (
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

type Message struct {
	kafka.Message
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
	return Message{msg}
}

// EmitRowLag will diff against the partition's high watermark and the message's offset
func (m Message) EmitRowLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.GaugeWithSample(
		"row.lag",
		float64(m.HighWaterMark-m.Offset),
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
	return m.Time
}
