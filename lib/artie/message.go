package artie

import (
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
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

type KafkaGoMessage struct {
	message kafka.Message
}

func NewKafkaGoMessage(msg kafka.Message) KafkaGoMessage {
	return KafkaGoMessage{message: msg}
}

func (m KafkaGoMessage) GetMessage() kafka.Message {
	return m.message
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

func (m KafkaGoMessage) HighWaterMark() int64 {
	return m.message.HighWaterMark
}

type FranzGoMessage struct {
	message kgo.Record
}

func NewFranzGoMessage(msg kgo.Record) FranzGoMessage {
	return FranzGoMessage{message: msg}
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
