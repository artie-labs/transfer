package kafkalib

import (
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

type ctxKey string

const consumer ctxKey = "consumer"

type Consumer interface {
	Close() (err error)
	ReadMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type TopicsToConsumerProvider struct {
	data map[string]Consumer
	sync.Mutex
}

func NewTopicsToConsumerProvider() *TopicsToConsumerProvider {
	return &TopicsToConsumerProvider{
		data: make(map[string]Consumer),
	}
}

func (t *TopicsToConsumerProvider) InjectIntoContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, consumer, t)
}

func GetTopicsToConsumerProviderFromContext(ctx context.Context) (*TopicsToConsumerProvider, bool) {
	provider, ok := ctx.Value(consumer).(*TopicsToConsumerProvider)
	return provider, ok
}

func (t *TopicsToConsumerProvider) Add(topic string, consumer Consumer) error {
	t.Lock()
	defer t.Unlock()

	if _, ok := t.data[topic]; ok {
		return fmt.Errorf("topic %q already exists", topic)
	}

	t.data[topic] = consumer
	return nil
}

func (t *TopicsToConsumerProvider) CommitMessage(ctx context.Context, topic string, msg kafka.Message) error {
	t.Lock()
	defer t.Unlock()

	if _, ok := t.data[topic]; !ok {
		return fmt.Errorf("topic %q does not exist", topic)
	}

	return t.data[topic].CommitMessages(ctx, msg)
}
