package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
)

var topicToConsumer *TopicToConsumer

func NewTopicToConsumer() *TopicToConsumer {
	return &TopicToConsumer{
		topicToConsumer: make(map[string]kafkalib.Consumer),
	}
}

// FranzGoConsumer wraps franz-go client to implement the Consumer interface
type FranzGoConsumer struct {
	client  *kgo.Client
	groupID string
}

func (f *FranzGoConsumer) Close() error {
	f.client.Close()
	return nil
}

func (f *FranzGoConsumer) ReadMessage(ctx context.Context) (*kgo.Record, error) {
	// Poll with a reasonable timeout to avoid tight loops
	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	fetches := f.client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		// Log the error for debugging
		slog.Debug("Error polling fetches", slog.Any("err", errs[0].Err))
		return nil, errs[0].Err
	}

	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		slog.Debug("Received message",
			slog.String("topic", record.Topic),
			slog.Int("partition", int(record.Partition)),
			slog.Int64("offset", record.Offset))
		return record, nil
	}

	// Return nil when no messages available (after timeout)
	return nil, nil
}

func (f *FranzGoConsumer) CommitMessages(ctx context.Context, msgs ...*kgo.Record) error {
	// franz-go handles auto-commit by default, but we can also commit manually
	f.client.MarkCommitRecords(msgs...)
	return f.client.CommitMarkedOffsets(ctx)
}

// Config returns a config-like structure for compatibility
func (f *FranzGoConsumer) Config() struct{ GroupID string } {
	return struct{ GroupID string }{GroupID: f.groupID}
}

type TopicToConsumer struct {
	topicToConsumer map[string]kafkalib.Consumer
	sync.RWMutex
}

func (t *TopicToConsumer) Add(topic string, consumer kafkalib.Consumer) {
	t.Lock()
	defer t.Unlock()
	t.topicToConsumer[topic] = consumer
}

func (t *TopicToConsumer) Get(topic string) kafkalib.Consumer {
	t.RLock()
	defer t.RUnlock()
	return t.topicToConsumer[topic]
}

func StartConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client) {
	kafkaConn := kafkalib.NewConnection(cfg.Kafka.EnableAWSMSKIAM, cfg.Kafka.DisableTLS, cfg.Kafka.Username, cfg.Kafka.Password, kafkalib.DefaultTimeout)
	slog.Info("Starting Kafka consumer...",
		slog.Any("config", cfg.Kafka),
		slog.Any("authMechanism", kafkaConn.Mechanism()),
	)

	brokers := cfg.Kafka.BootstrapServers(true)
	clientOpts, err := kafkaConn.ClientOptions(ctx, brokers)
	if err != nil {
		logger.Panic("Failed to create Kafka client options", slog.Any("err", err))
	}

	tcFmtMap := NewTcFmtMap()
	topicToConsumer = NewTopicToConsumer()
	var topics []string
	for _, topicConfig := range cfg.Kafka.TopicConfigs {
		tcFmtMap.Add(topicConfig.Topic, TopicConfigFormatter{
			tc:     *topicConfig,
			Format: format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic),
		})
		topics = append(topics, topicConfig.Topic)
	}

	// Create a SINGLE client for all topics in the consumer group
	// This is the correct approach for franz-go consumer groups
	clientOpts = append(clientOpts,
		kgo.ConsumerGroup(cfg.Kafka.GroupID),
		kgo.ConsumeTopics(topics...), // Consume ALL topics with one client
		// Start from beginning if no committed offset (good for dev/testing)
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Consumer group lifecycle callbacks with detailed logging
		kgo.OnPartitionsAssigned(func(ctx context.Context, c *kgo.Client, assigned map[string][]int32) {
			for topic, partitions := range assigned {
				slog.Info("🎉 Partitions assigned",
					slog.String("topic", topic),
					slog.Any("partitions", partitions),
					slog.String("groupID", cfg.Kafka.GroupID))
			}
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, revoked map[string][]int32) {
			for topic, partitions := range revoked {
				slog.Info("Partitions revoked",
					slog.String("topic", topic),
					slog.Any("partitions", partitions),
					slog.String("groupID", cfg.Kafka.GroupID))
			}
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, c *kgo.Client, lost map[string][]int32) {
			for topic, partitions := range lost {
				slog.Warn("⚠️ Partitions lost",
					slog.String("topic", topic),
					slog.Any("partitions", partitions),
					slog.String("groupID", cfg.Kafka.GroupID))
			}
		}),
	)

	// Create the single shared client
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		logger.Panic("Failed to create Kafka client", slog.Any("err", err))
	}
	defer client.Close()

	// Create a single consumer for all topics
	kafkaConsumer := &FranzGoConsumer{
		client:  client,
		groupID: cfg.Kafka.GroupID,
	}

	// Add the consumer for each topic (for backward compatibility)
	for _, topic := range topics {
		topicToConsumer.Add(topic, kafkaConsumer)
	}

	slog.Info("🚀 Created shared Kafka consumer",
		slog.Any("topics", topics),
		slog.String("groupID", cfg.Kafka.GroupID),
		slog.Any("brokers", brokers))

	// Single consumer loop for all topics
	for {
		kafkaMsg, err := kafkaConsumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled, exit gracefully
				slog.Info("Consumer context cancelled")
				return
			}
			slog.Warn("Failed to read kafka message", slog.Any("err", err))
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if kafkaMsg == nil {
			// No message available after timeout, continue polling
			continue
		}

		if len(kafkaMsg.Value) == 0 {
			slog.Debug("Found a tombstone message, skipping...", artie.BuildLogFields(kafkaMsg)...)
			continue
		}

		msg := artie.NewMessage(kafkaMsg)
		args := processArgs{
			Msg:                    msg,
			GroupID:                kafkaConsumer.Config().GroupID,
			TopicToConfigFormatMap: tcFmtMap,
		}

		tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
		if err != nil {
			logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", kafkaMsg.Topic))
		}

		msg.EmitIngestionLag(metricsClient, cfg.Mode, kafkaConsumer.Config().GroupID, tableID.Table)
		msg.EmitRowLag(metricsClient, cfg.Mode, kafkaConsumer.Config().GroupID, tableID.Table)
	}
}
