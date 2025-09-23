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

type Consumer[M any] interface {
	Close() (err error)
	FetchMessage(ctx context.Context) (M, error)
	CommitMessages(ctx context.Context, msgs ...M) error
}

type ConsumerProvider[M any] struct {
	mu                       sync.Mutex
	topic                    string
	groupID                  string
	partitionToAppliedOffset map[int]M

	Consumer[M]
}

func (c *ConsumerProvider[M]) SetPartitionToAppliedOffsetTest(msg M) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitionToAppliedOffset[getPartition(msg)] = msg
}

func NewConsumerProviderForTest[M any](consumer Consumer[M], topic string, groupID string) *ConsumerProvider[M] {
	return &ConsumerProvider[M]{
		Consumer:                 consumer,
		topic:                    topic,
		groupID:                  groupID,
		partitionToAppliedOffset: make(map[int]M),
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

			// This will ensure that we're watching metadata updates from Kafka.
			// When there's a partition change, we'll rediscover and refresh our assignment and connections automatically without a restart.
			WatchPartitionChanges: true,
		}

		ctx = context.WithValue(ctx, BuildContextKey(topicConfig.Topic), &ConsumerProvider[kafka.Message]{
			Consumer:                 kafka.NewReader(kafkaCfg),
			topic:                    topicConfig.Topic,
			groupID:                  cfg.GroupID,
			partitionToAppliedOffset: make(map[int]kafka.Message),
		})
	}

	return ctx, nil
}

func (c *ConsumerProvider[M]) LockAndProcess(ctx context.Context, lock bool, do func() error) error {
	if lock {
		c.mu.Lock()
		defer c.mu.Unlock()
	}

	if err := do(); err != nil {
		return fmt.Errorf("failed to process: %w", err)
	}

	return nil
}

func (c *ConsumerProvider[M]) FetchMessageAndProcess(ctx context.Context, do func(M) error) error {
	msg, err := c.Consumer.FetchMessage(ctx)
	if err != nil {
		return NewFetchMessageError(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if appliedMsg, ok := c.partitionToAppliedOffset[getPartition(msg)]; ok {
		if getOffset(appliedMsg) >= getOffset(msg) {
			// We should skip this message because we have already processed it.
			return nil
		}
	}

	if err := do(msg); err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	c.partitionToAppliedOffset[getPartition(msg)] = msg
	return nil
}

func GetConsumerFromContext[M any](ctx context.Context, topic string) (*ConsumerProvider[M], error) {
	value := ctx.Value(BuildContextKey(topic))
	consumer, ok := value.(*ConsumerProvider[M])
	if !ok {
		return nil, fmt.Errorf("consumer not found for topic %q, got: %T", topic, value)
	}

	return consumer, nil
}

func (c *ConsumerProvider[M]) CommitMessage(ctx context.Context) error {
	var msgs []M

	partitionToOffset := make(map[int]int64)
	// Gather all the messages across all the partitions we have seen
	for _, msg := range c.partitionToAppliedOffset {
		partitionToOffset[getPartition(msg)] = getOffset(msg)
		msgs = append(msgs, msg)
	}

	// Commit all of them
	if err := c.Consumer.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("failed to commit messages: %w", err)
	}

	slog.Info("Committed messages", slog.String("topic", c.topic), slog.Any("partitionToOffset", partitionToOffset))
	return nil
}

func (c *ConsumerProvider[M]) GetGroupID() string {
	return c.groupID
}

func getPartition[M any](msg M) int {
	switch m := any(msg).(type) {
	case kafka.Message:
		return m.Partition
	default:
		return 0
	}
}

func getOffset[M any](msg M) int64 {
	switch m := any(msg).(type) {
	case kafka.Message:
		return m.Offset
	default:
		return 0
	}
}
