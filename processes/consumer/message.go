package consumer

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

func CommitOffset(ctx context.Context, topic string, partitionsToOffset map[string]Message) error {
	var err error
	for _, msg := range partitionsToOffset {
		if msg.kafkaMsg != nil {
			err = topicToConsumer[topic].CommitMessages(ctx, *msg.kafkaMsg)
			if err != nil {
				return err
			}
		}

		if msg.pubsubMsg != nil {
			msg.pubsubMsg.Ack()
		}
	}

	return err
}

type pubsubWrapper struct {
	topic string
	*pubsub.Message
}

type Message struct {
	kafkaMsg  *kafka.Message
	pubsubMsg *pubsubWrapper
}

func NewMessage(kafkaMsg *kafka.Message, pubsubMsg *pubsub.Message, topic string) Message {
	return Message{
		kafkaMsg: kafkaMsg,
		pubsubMsg: &pubsubWrapper{
			Message: pubsubMsg,
			topic:   topic,
		},
	}
}

func (m *Message) PublishTime() time.Time {
	if m.kafkaMsg != nil {
		return m.kafkaMsg.Time
	}

	if m.pubsubMsg != nil {
		return m.pubsubMsg.PublishTime
	}

	return time.Time{}
}

func (m *Message) Topic() string {
	if m.kafkaMsg != nil {
		return m.kafkaMsg.Topic
	}

	if m.pubsubMsg != nil {
		return m.pubsubMsg.topic
	}

	return ""
}

func (m *Message) Partition() string {
	if m.kafkaMsg != nil {
		return fmt.Sprint(m.kafkaMsg.Partition)
	}

	if m.pubsubMsg != nil {
		// Pub/Sub doesn't have partitions.
		return "no_partition"
	}

	return ""
}

func (m *Message) Key() []byte {
	if m.kafkaMsg != nil {
		return m.kafkaMsg.Key
	}

	if m.pubsubMsg != nil {
		return []byte(m.pubsubMsg.OrderingKey)
	}

	return nil
}

func (m *Message) Value() []byte {
	if m.kafkaMsg != nil {
		return m.kafkaMsg.Value
	}

	if m.pubsubMsg != nil {
		return m.pubsubMsg.Data
	}

	return nil
}
