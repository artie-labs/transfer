package kafkalib

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer interface {
	Close() (err error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	GetVersion() int64
}
