package artie

import (
	"cloud.google.com/go/pubsub"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
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
	return Message{
		KafkaMsg: kafkaMsg,
		PubSub: &pubsubWrapper{
			Message: pubsubMsg,
			topic:   topic,
		},
	}
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
