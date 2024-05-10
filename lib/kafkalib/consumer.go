package kafkalib

import (
	"context"
	"errors"

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

func ShouldReload(err error) bool {
	if err == nil {
		return false
	}

	// Kafka Segment Go doesn't handle reloading the client: https://github.com/segmentio/kafka-go/issues/833
	// [27] Rebalance In Progress: the coordinator has begun rebalancing the group, the client should rejoin the group
	return errors.Is(err, kafka.RebalanceInProgress)
}

func (r *Reader) Reload() {
	// Close, then reload.
	_ = r.Close()
	r.Reader = kafka.NewReader(r.config)
}
