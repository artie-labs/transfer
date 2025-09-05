package kafkalib

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer interface {
	Close() (err error)
	ReadMessage(ctx context.Context) (*kgo.Record, error)
	CommitMessages(ctx context.Context, msgs ...*kgo.Record) error
}
