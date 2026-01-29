package artie

import (
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Message interface {
	PublishTime() time.Time
	Topic() string
	Partition() int
	Offset() int64
	Key() []byte
	Value() []byte
	HighWaterMark() int64
}

func BuildLogFields(msg Message) []any {
	return []any{
		slog.String("topic", msg.Topic()),
		slog.Int64("offset", msg.Offset()),
		slog.String("key", string(msg.Key())),
		slog.String("value", string(msg.Value())),
	}
}

type FranzGoMessage struct {
	message       kgo.Record
	highWatermark int64
}

func NewFranzGoMessage(msg kgo.Record, highWatermark int64) Message {
	return FranzGoMessage{message: msg, highWatermark: highWatermark}
}

func (m FranzGoMessage) GetMessage() kgo.Record {
	return m.message
}

func (m FranzGoMessage) PublishTime() time.Time {
	return m.message.Timestamp
}

func (m FranzGoMessage) Topic() string {
	return m.message.Topic
}

func (m FranzGoMessage) Partition() int {
	return int(m.message.Partition)
}

func (m FranzGoMessage) Offset() int64 {
	return m.message.Offset
}

func (m FranzGoMessage) Key() []byte {
	return m.message.Key
}

func (m FranzGoMessage) Value() []byte {
	return m.message.Value
}

func (m FranzGoMessage) HighWaterMark() int64 {
	return m.highWatermark
}
