package kafkalib

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/segmentio/kafka-go"
)

type ctxKey string

func BuildContextKey(topic string) ctxKey {
	return ctxKey(fmt.Sprintf("consumer-%s", topic))
}

type Consumer interface {
	Close() (err error)
	FetchMessage(ctx context.Context) (artie.Message, error)
	CommitMessages(ctx context.Context, msgs ...artie.Message) error
}

type KafkaGoConsumer struct {
	*kafka.Reader
}

func (k KafkaGoConsumer) CommitMessages(ctx context.Context, msgs ...artie.Message) error {
	// TODO: Find a better way to get an array of kafka.Message without allocating a new slice.
	kafkaMsgs := make([]kafka.Message, len(msgs))
	for i, msg := range msgs {
		if kMsg, ok := msg.(artie.KafkaGoMessage); ok {
			kafkaMsgs[i] = kMsg.GetMessage()
		} else {
			return fmt.Errorf("message is not of type artie.KafkaGoMessage: %T", msg)
		}
	}
	return k.Reader.CommitMessages(ctx, kafkaMsgs...)
}

func (k KafkaGoConsumer) FetchMessage(ctx context.Context) (artie.Message, error) {
	msg, err := k.Reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	return artie.NewKafkaGoMessage(msg), nil
}

type ConsumerProvider struct {
	mu                       sync.Mutex
	topic                    string
	groupID                  string
	partitionToAppliedOffset map[int]artie.Message

	Consumer
}

func (c *ConsumerProvider) SetPartitionToAppliedOffsetTest(msg artie.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitionToAppliedOffset[msg.Partition()] = msg
}

func NewConsumerProviderForTest(consumer Consumer, topic string, groupID string) *ConsumerProvider {
	return &ConsumerProvider{
		Consumer:                 consumer,
		topic:                    topic,
		groupID:                  groupID,
		partitionToAppliedOffset: make(map[int]artie.Message),
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

		ctx = context.WithValue(ctx, BuildContextKey(topicConfig.Topic), &ConsumerProvider{
			Consumer:                 &KafkaGoConsumer{kafka.NewReader(kafkaCfg)},
			topic:                    topicConfig.Topic,
			groupID:                  cfg.GroupID,
			partitionToAppliedOffset: make(map[int]artie.Message),
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
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	msg, err := c.Consumer.FetchMessage(ctx)
	if err != nil {
		return NewFetchMessageError(err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if appliedMsg, ok := c.partitionToAppliedOffset[msg.Partition()]; ok {
		if appliedMsg.Offset() >= msg.Offset() {
			// We should skip this message because we have already processed it.
			return nil
		}
	}

	if err := do(msg); err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	c.partitionToAppliedOffset[msg.Partition()] = msg
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
	var msgs []artie.Message

	partitionToOffset := make(map[int]int64)
	// Gather all the messages across all the partitions we have seen
	for _, msg := range c.partitionToAppliedOffset {
		partitionToOffset[msg.Partition()] = msg.Offset()
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
