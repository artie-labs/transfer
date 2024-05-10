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

type Reader struct {
	*kafka.Reader
	config kafka.ReaderConfig
}

func NewReader(config kafka.ReaderConfig) *Reader {
	return &Reader{
		Reader: kafka.NewReader(config),
		config: config,
	}
}

func (r *Reader) Reload() error {
	// Close, then reload.
	_ = r.Close()
	r.Reader = kafka.NewReader(r.config)
	return nil
}
