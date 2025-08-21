package artie

import (
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

type Message struct {
	message *kgo.Record
}

func (m Message) GetMessage() *kgo.Record {
	return m.message
}

func BuildLogFields(msg *kgo.Record) []any {
	return []any{
		slog.String("topic", msg.Topic),
		slog.Int64("offset", msg.Offset),
		slog.String("key", string(msg.Key)),
		slog.String("value", string(msg.Value)),
	}
}

func NewMessage(msg *kgo.Record) Message {
	return Message{message: msg}
}

// EmitRowLag will diff against the partition's high watermark and the message's offset
// Note: franz-go doesn't provide high watermark in the record, so we'll emit 0 for now
// This would need to be calculated separately if needed
func (m Message) EmitRowLag(metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.GaugeWithSample(
		"row.lag",
		float64(0), // TODO: Calculate lag separately with franz-go
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
	return m.message.Timestamp
}

func (m Message) Topic() string {
	return m.message.Topic
}

func (m Message) Partition() int {
	return int(m.message.Partition)
}

func (m Message) Offset() int64 {
	return m.message.Offset
}

func (m Message) Key() []byte {
	return m.message.Key
}

func (m Message) Value() []byte {
	return m.message.Value
}
