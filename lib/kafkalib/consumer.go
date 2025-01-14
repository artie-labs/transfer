package kafkalib

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Close() (err error)
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}
