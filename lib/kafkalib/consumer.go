package kafkalib

import (
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

type ctxKey string

func BuildContextKey(topic string) ctxKey {
	return ctxKey(fmt.Sprintf("consumer-%s", topic))
}

type Consumer interface {
	Close() (err error)
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
}

type ConsumerProvider struct {
	mu      sync.Mutex
	groupID string
	offset  int64

	Consumer
}

func NewConsumerProviderForTest(consumer Consumer, groupID string) *ConsumerProvider {
	return &ConsumerProvider{
		Consumer: consumer,
		groupID:  groupID,
	}
}

func InjectConsumerProvidersIntoContext(ctx context.Context, cfg *Kafka) (context.Context, error) {
	kafkaConn := NewConnection(cfg.EnableAWSMSKIAM, cfg.DisableTLS, cfg.Username, cfg.Password, DefaultTimeout)
	dialer, err := kafkaConn.Dialer(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka dialer: %w", err)
	}

	for _, topicConfig := range cfg.TopicConfigs {
		kafkaCfg := kafka.ReaderConfig{
			GroupID: cfg.GroupID,
			Dialer:  dialer,
			Topic:   topicConfig.Topic,
			Brokers: cfg.BootstrapServers(true),
		}

		ctx = context.WithValue(ctx, BuildContextKey(topicConfig.Topic), &ConsumerProvider{Consumer: kafka.NewReader(kafkaCfg), groupID: cfg.GroupID})
	}

	return ctx, nil
}

func (c *ConsumerProvider) LockAndProcess(ctx context.Context, lock bool, do func() error) error {
	if lock {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if err := do(); err != nil {
		return fmt.Errorf("failed to process: %w", err)
	}

	return nil
}

func (c *ConsumerProvider) FetchMessageAndProcess(ctx context.Context, do func(kafka.Message) error) error {
	msg, err := c.Consumer.FetchMessage(ctx)
	if err != nil {
		return NewFetchMessageError(err)
	}

	fmt.Println("fetching message", msg.Offset, c.offset)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.offset > msg.Offset {
		fmt.Println("skipping message", msg.Offset, c.offset)
		// We should skip this message because we have already processed it.
		return nil
	}

	if err := do(msg); err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	// Set the offset to the last processed message.
	c.offset = msg.Offset
	return nil
}

func GetConsumerFromContext(ctx context.Context, topic string) (*ConsumerProvider, error) {
	value := ctx.Value(BuildContextKey(topic))
	consumer, ok := value.(*ConsumerProvider)
	if !ok {
		return nil, fmt.Errorf("consumer not found for topic %q, got: %T", topic, value)
	}

	return consumer, nil
}

func (c *ConsumerProvider) CommitMessage(ctx context.Context, msg kafka.Message) error {
	return c.Consumer.CommitMessages(ctx, msg)
}

func (c *ConsumerProvider) GetGroupID() string {
	return c.groupID
}
