package kafkalib

import (
	"context"
	"fmt"
	"log/slog"
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
	mu                       sync.Mutex
	topic                    string
	groupID                  string
	partitionToAppliedOffset map[int]kafka.Message

	Consumer
}

func (c *ConsumerProvider) SetPartitionToAppliedOffsetTest(msg kafka.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitionToAppliedOffset[msg.Partition] = msg
}

func NewConsumerProviderForTest(consumer Consumer, groupID string) *ConsumerProvider {
	return &ConsumerProvider{
		Consumer:                 consumer,
		groupID:                  groupID,
		partitionToAppliedOffset: make(map[int]kafka.Message),
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

		ctx = context.WithValue(ctx, BuildContextKey(topicConfig.Topic), &ConsumerProvider{
			Consumer:                 kafka.NewReader(kafkaCfg),
			topic:                    topicConfig.Topic,
			groupID:                  cfg.GroupID,
			partitionToAppliedOffset: make(map[int]kafka.Message),
		})
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

	c.mu.Lock()
	defer c.mu.Unlock()

	if appliedMsg, ok := c.partitionToAppliedOffset[msg.Partition]; ok {
		if appliedMsg.Offset >= msg.Offset {
			// We should skip this message because we have already processed it.
			return nil
		}
	}

	if err := do(msg); err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	c.partitionToAppliedOffset[msg.Partition] = msg
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

func (c *ConsumerProvider) CommitMessage(ctx context.Context) error {
	var msgs []kafka.Message

	partitionToOffset := make(map[int]int64)
	// Gather all the messages across all the partitions we have seen
	for _, msg := range c.partitionToAppliedOffset {
		partitionToOffset[msg.Partition] = msg.Offset
		msgs = append(msgs, msg)
	}

	// Commit all of them
	if err := c.Consumer.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("failed to commit messages: %w", err)
	}

	slog.Info("Committed messages", slog.String("topic", c.topic), slog.Any("partitionToOffset", partitionToOffset))
	return nil
}

func (c *ConsumerProvider) GetGroupID() string {
	return c.groupID
}
