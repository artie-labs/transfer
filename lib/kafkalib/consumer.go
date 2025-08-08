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
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type TopicsToConsumerProvider struct {
	data map[string]Consumer
	groupID string
	sync.Mutex
}

func NewTopicsToConsumerProvider(ctx context.Context, cfg *Kafka) (*TopicsToConsumerProvider, error) {
	provider := &TopicsToConsumerProvider{
		data: make(map[string]Consumer),
		groupID: cfg.GroupID,
	}

	kafkaConn := NewConnection(cfg.Kafka.EnableAWSMSKIAM, cfg.Kafka.DisableTLS, cfg.Kafka.Username, cfg.Kafka.Password, DefaultTimeout)
	dialer, err := kafkaConn.Dialer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka dialer: %w", err)
	}

	for _, topicConfig := range cfg.TopicConfigs {
		kafkaCfg := kafka.ReaderConfig{
			GroupID: cfg.Kafka.GroupID,
			Dialer:  dialer,
			Topic:   topic,
			Brokers: cfg.Kafka.BootstrapServers(true),
		}

		provider.Add(topicConfig.Topic, kafka.NewReader(kafkaCfg))
	}

	return provider, nil
}

func (t *TopicsToConsumerProvider) GroupID() string {
	return t.groupID
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

func (t *TopicsToConsumerProvider) FetchMessage(ctx context.Context, topic string) (kafka.Message, error) {
	t.Lock()
	defer t.Unlock()

	if _, ok := t.data[topic]; !ok {
		return kafka.Message{}, fmt.Errorf("topic %q does not exist", topic)
	},

	return t.data[topic].FetchMessage(ctx)
}
