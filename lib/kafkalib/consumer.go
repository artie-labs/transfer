package kafkalib

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/artie-labs/transfer/lib/artie"
)

const (
	FetchMessageTimeout     = 5 * time.Minute
	TopicExistsPollInterval = 5 * time.Minute
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

type FranzGoConsumer struct {
	client  *kgo.Client
	groupID string
	topic   string
	// Map to store high watermarks by topic-partition key
	highWatermarks map[string]int64
	currentIter    *kgo.FetchesRecordIter
}

func GetHighWatermarkMapKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func NewFranzGoConsumer(client *kgo.Client, groupID, topic string) Consumer {
	return &FranzGoConsumer{
		client:         client,
		groupID:        groupID,
		topic:          topic,
		highWatermarks: make(map[string]int64),
	}
}

func (f FranzGoConsumer) Client() *kgo.Client {
	return f.client
}

func (f *FranzGoConsumer) GetHighWatermark(record kgo.Record) int64 {
	if hwm, exists := f.highWatermarks[GetHighWatermarkMapKey(record.Topic, record.Partition)]; exists {
		return hwm
	}
	return 0 // Default to 0 if not found
}

func (f *FranzGoConsumer) Close() error {
	f.client.Close()
	return nil
}

func (f *FranzGoConsumer) FetchMessage(ctx context.Context) (artie.Message, error) {
	if f.currentIter != nil && !f.currentIter.Done() {
		record := f.currentIter.Next()
		slog.Debug("Received message",
			slog.String("topic", record.Topic),
			slog.Int("partition", int(record.Partition)),
			slog.Int64("offset", record.Offset))

		return artie.NewFranzGoMessage(*record, f.GetHighWatermark(*record)), nil
	}

	groupID, generation := f.client.GroupMetadata()
	slog.Debug("Polling topics", slog.Any("topics", f.client.GetConsumeTopics()), slog.String("groupID", groupID), slog.Int("generation", int(generation)))

	fetches := f.client.PollFetches(ctx)
	slog.Debug("done polling", "fetches", fetches, slog.Any("topics", f.client.GetConsumeTopics()), slog.String("groupID", groupID), slog.Int("generation", int(generation)))

	if errs := fetches.Errors(); len(errs) > 0 {
		var combinedErrors []error
		for _, err := range errs {
			combinedErrors = append(combinedErrors, err.Err)
		}
		return nil, errors.Join(combinedErrors...)
	}

	// Since HWM is a field on the Partition and not on every kgo.Record,
	// we need to iterate over the partitions and update the high watermark map.
	fetches.EachTopic(func(topic kgo.FetchTopic) {
		topic.EachPartition(func(partition kgo.FetchPartition) {
			f.highWatermarks[GetHighWatermarkMapKey(topic.Topic, partition.Partition)] = partition.HighWatermark
		})
	})

	f.currentIter = fetches.RecordIter()
	if f.currentIter.Done() {
		return nil, fmt.Errorf("no messages found")
	}

	record := f.currentIter.Next()

	return artie.NewFranzGoMessage(*record, f.GetHighWatermark(*record)), nil
}

// TopicExists checks if a topic exists in the Kafka cluster using metadata request.
func TopicExists(ctx context.Context, client *kgo.Client, topic string) (bool, error) {
	req := kmsg.NewMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topic)}}

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return false, fmt.Errorf("failed to fetch metadata: %w", err)
	}

	for _, t := range resp.Topics {
		if t.Topic != nil && *t.Topic == topic {
			if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
				if errors.Is(err, kerr.UnknownTopicOrPartition) {
					return false, nil
				}
				return false, fmt.Errorf("topic metadata error for %q: %w", topic, err)
			}
			return true, nil
		}
	}
	return false, nil
}

// WaitForTopicToExist polls until the topic exists or the context is cancelled.
func WaitForTopicToExist(ctx context.Context, client *kgo.Client, topic string) error {
	for {
		exists, err := TopicExists(ctx, client, topic)
		if err != nil {
			return fmt.Errorf("failed to check topic existence: %w", err)
		}
		if exists {
			slog.Info("Topic exists, proceeding with consumer setup", slog.String("topic", topic))
			return nil
		}

		slog.Info("Topic does not exist yet, waiting...", slog.String("topic", topic), slog.Duration("pollInterval", TopicExistsPollInterval))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(TopicExistsPollInterval):
		}
	}
}

func (f *FranzGoConsumer) CommitMessages(ctx context.Context, msgs ...artie.Message) error {
	offsetsToCommit := make(map[string]map[int32]kgo.EpochOffset)

	for i, msg := range msgs {
		slog.Debug("Processing message for commit",
			slog.Int("msgIndex", i),
			slog.String("topic", msg.Topic()),
			slog.Int("partition", int(msg.Partition())),
			slog.Int64("currentOffset", msg.Offset()),
			slog.Int64("commitOffset", msg.Offset()+1)) // Commit next offset

		if offsetsToCommit[msg.Topic()] == nil {
			offsetsToCommit[msg.Topic()] = make(map[int32]kgo.EpochOffset)
		}

		// Kafka expects the next offset to read, so we commit offset + 1
		offsetsToCommit[msg.Topic()][int32(msg.Partition())] = kgo.EpochOffset{
			Epoch:  -1, // Use -1 for unknown epoch (franz-go will handle this)
			Offset: msg.Offset() + 1,
		}
	}

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		// Check consumer group status before commit
		groupID, generation := f.client.GroupMetadata()
		slog.Debug("Committing explicit offsets",
			slog.String("groupID", groupID),
			slog.Int("generation", int(generation)),
			slog.Any("offsetsToCommit", offsetsToCommit))
	}

	var commitError error
	f.client.CommitOffsetsSync(ctx, offsetsToCommit, func(client *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		commitError = err
		if err != nil {
			slog.Error("Sync commit callback failed", slog.Any("err", err))
		} else {
			if slog.Default().Enabled(ctx, slog.LevelDebug) {
				slog.Debug("Sync commit callback succeeded",
					slog.Int("numTopics", len(resp.Topics)))
				for _, topic := range resp.Topics {
					for _, partition := range topic.Partitions {
						slog.Debug("Committed offset for partition",
							slog.String("topic", topic.Topic),
							slog.Int("partition", int(partition.Partition)),
							slog.Any("errorCode", partition.ErrorCode))
					}
				}
			}
		}
	})

	if commitError != nil {
		return fmt.Errorf("commit failed via callback: %w", commitError)
	}

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		committedOffsets := f.client.CommittedOffsets()
		slog.Debug("CommitOffsets completed successfully",
			slog.Any("committedOffsets", committedOffsets))
	}

	return nil
}

type ConsumerProvider struct {
	mu                       sync.Mutex
	topic                    string
	groupID                  string
	partitionToAppliedOffset map[int]artie.Message
	client                   *kgo.Client // For FranzGo consumers

	Consumer
}

// WaitForTopic waits for the topic to exist. Only supported for FranzGo consumers.
func (c *ConsumerProvider) WaitForTopic(ctx context.Context) error {
	if c.client == nil {
		return nil // skip if no franz-go client is set
	}
	return WaitForTopicToExist(ctx, c.client, c.topic)
}

func (c *ConsumerProvider) SetPartitionToAppliedOffsetTest(msg artie.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partitionToAppliedOffset[msg.Partition()] = msg
}

func NewConsumerProviderForTest(consumer Consumer, topic, groupID string) *ConsumerProvider {
	return &ConsumerProvider{
		Consumer:                 consumer,
		topic:                    topic,
		groupID:                  groupID,
		partitionToAppliedOffset: make(map[int]artie.Message),
	}
}

func InjectFranzGoConsumerProvidersIntoContext(ctx context.Context, cfg *Kafka) (context.Context, error) {
	kafkaConn := NewConnection(cfg.EnableAWSMSKIAM, cfg.DisableTLS, cfg.Username, cfg.Password, DefaultTimeout)
	brokers := cfg.BootstrapServers(true)

	var createdClients []*kgo.Client
	closeClients := func() {
		for _, c := range createdClients {
			c.Close()
		}
	}

	// Create separate clients for each topic
	for _, topicConfig := range cfg.TopicConfigs {
		clientOpts, err := kafkaConn.ClientOptions(ctx, brokers)
		if err != nil {
			closeClients()
			return nil, fmt.Errorf("failed to create Kafka client options for topic %s: %w", topicConfig.Topic, err)
		}

		clientOpts = append(clientOpts,
			kgo.ConsumerGroup(cfg.GroupID),
			kgo.ConsumeTopics(topicConfig.Topic), // Consume only this specific topic
			kgo.DisableAutoCommit(),
			// Set session timeout for consumer group heartbeats
			kgo.SessionTimeout(30*time.Second),
			// Set heartbeat interval
			kgo.HeartbeatInterval(3*time.Second),
			// Ensure we allow time for rebalancing
			kgo.RebalanceTimeout(30*time.Second),
			// Consumer group lifecycle callbacks with detailed logging
			kgo.OnPartitionsAssigned(func(ctx context.Context, c *kgo.Client, assigned map[string][]int32) {
				for topic, partitions := range assigned {
					// Check group metadata during assignment for debugging
					actualGroupID, generation := c.GroupMetadata()
					slog.Info("Partitions assigned",
						slog.String("topic", topic),
						slog.Any("partitions", partitions),
						slog.String("expectedGroupID", cfg.GroupID),
						slog.String("actualGroupID", actualGroupID),
						slog.Int("generation", int(generation)))
				}
			}),
			kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
				for topic, partitions := range revoked {
					slog.Info("Partitions revoked",
						slog.String("topic", topic),
						slog.Any("partitions", partitions),
						slog.String("groupID", cfg.GroupID))
				}
			}),
			kgo.OnPartitionsLost(func(ctx context.Context, c *kgo.Client, lost map[string][]int32) {
				for topic, partitions := range lost {
					slog.Warn("Partitions lost",
						slog.String("topic", topic),
						slog.Any("partitions", partitions),
						slog.String("groupID", cfg.GroupID))
				}
			}),
		)

		// Apply optional fetch tuning settings if configured
		if cfg.FetchMaxBytes > 0 {
			clientOpts = append(clientOpts, kgo.FetchMaxBytes(cfg.FetchMaxBytes))
		}
		if cfg.FetchMaxPartitionBytes > 0 {
			clientOpts = append(clientOpts, kgo.FetchMaxPartitionBytes(cfg.FetchMaxPartitionBytes))
		}
		if cfg.FetchMinBytes > 0 {
			clientOpts = append(clientOpts, kgo.FetchMinBytes(cfg.FetchMinBytes))
		}
		if cfg.FetchMaxWaitMs > 0 {
			clientOpts = append(clientOpts, kgo.FetchMaxWait(time.Duration(cfg.FetchMaxWaitMs)*time.Millisecond))
		}

		client, err := kgo.NewClient(clientOpts...)
		if err != nil {
			closeClients()
			return nil, fmt.Errorf("failed to create Kafka client for topic %s: %w", topicConfig.Topic, err)
		}
		createdClients = append(createdClients, client)

		slog.Info("Created Kafka consumer for topic",
			slog.String("topic", topicConfig.Topic),
			slog.String("groupID", cfg.GroupID),
			slog.Any("brokers", brokers))

		ctx = context.WithValue(ctx, BuildContextKey(topicConfig.Topic), &ConsumerProvider{
			Consumer:                 NewFranzGoConsumer(client, cfg.GroupID, topicConfig.Topic),
			topic:                    topicConfig.Topic,
			groupID:                  cfg.GroupID,
			partitionToAppliedOffset: make(map[int]artie.Message),
			client:                   client,
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

func (c *ConsumerProvider) FetchMessageAndProcess(ctx context.Context, do func(artie.Message) error) error {
	ctx, cancel := context.WithTimeout(ctx, FetchMessageTimeout)
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
