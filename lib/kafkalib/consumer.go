package kafkalib

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Consumer interface {
	Close() (err error)
	SubscribeTopics(topics []string, rebalanceCb kafka.RebalanceCb) (err error)
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	CommitOffsets(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error)
}
