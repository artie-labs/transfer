package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

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
	client       *kgo.Client
	groupID      string
	recordBuffer []*kgo.Record
	bufferIndex  int
}

func (f *FranzGoConsumer) Close() error {
	f.client.Close()
	return nil
}

func (f *FranzGoConsumer) ReadMessage(ctx context.Context) (*kgo.Record, error) {
	// First, check if we have buffered records from a previous fetch
	if f.bufferIndex < len(f.recordBuffer) {
		record := f.recordBuffer[f.bufferIndex]
		f.bufferIndex++
		slog.Info("Received message",
			slog.String("topic", record.Topic),
			slog.Int("partition", int(record.Partition)),
			slog.Int64("offset", record.Offset))
		return record, nil
	}

	// Buffer is empty or exhausted, need to poll for new records
	// Poll with a longer timeout to allow for consumer group coordination
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	groupID, generation := f.client.GroupMetadata()
	slog.Debug("Polling topics", slog.Any("topics", f.client.GetConsumeTopics()), slog.String("groupID", groupID), slog.Int("generation", int(generation)))
	fetches := f.client.PollFetches(ctx)
	slog.Debug("done polling", "fetches", fetches, slog.Any("topics", f.client.GetConsumeTopics()), slog.String("groupID", groupID), slog.Int("generation", int(generation)))
	if errs := fetches.Errors(); len(errs) > 0 {
		// Don't log timeouts as warnings, they're normal
		if ctx.Err() != context.DeadlineExceeded {
			slog.Warn("Error polling fetches", slog.Any("err", errs[0].Err))
		}
		return nil, errs[0].Err
	}

	// Clear the buffer and collect all records from this fetch
	f.recordBuffer = f.recordBuffer[:0] // Clear slice but keep capacity
	f.bufferIndex = 0

	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		f.recordBuffer = append(f.recordBuffer, record)
	}

	// If no records were fetched, return nil (normal timeout case)
	if len(f.recordBuffer) == 0 {
		return nil, nil
	}

	// Return the first record from the newly filled buffer
	record := f.recordBuffer[0]
	f.bufferIndex = 1
	slog.Info("📨 Received message",
		slog.String("topic", record.Topic),
		slog.Int("partition", int(record.Partition)),
		slog.Int64("offset", record.Offset))
	return record, nil
}

func (f *FranzGoConsumer) CommitMessages(ctx context.Context, msgs ...*kgo.Record) error {
	// franz-go handles auto-commit by default, but we can also commit manually
	slog.Info("Committing messages using explicit offset method", slog.Int("numRecords", len(msgs)))

	// Build explicit offset map - Kafka expects the NEXT offset to read
	offsetsToCommit := make(map[string]map[int32]kgo.EpochOffset)

	for i, msg := range msgs {
		slog.Info("Processing message for commit",
			slog.Int("msgIndex", i),
			slog.String("topic", msg.Topic),
			slog.Int("partition", int(msg.Partition)),
			slog.Int64("currentOffset", msg.Offset),
			slog.Int64("commitOffset", msg.Offset+1)) // Commit next offset

		if offsetsToCommit[msg.Topic] == nil {
			offsetsToCommit[msg.Topic] = make(map[int32]kgo.EpochOffset)
		}

		// Kafka expects the next offset to read, so we commit offset + 1
		offsetsToCommit[msg.Topic][msg.Partition] = kgo.EpochOffset{
			Epoch:  -1, // Use -1 for unknown epoch (franz-go will handle this)
			Offset: msg.Offset + 1,
		}
	}

	// Check consumer group status before commit
	groupID, generation := f.client.GroupMetadata()
	slog.Info("Committing explicit offsets",
		slog.String("groupID", groupID),
		slog.Int("generation", int(generation)),
		slog.Any("offsetsToCommit", offsetsToCommit))

	// Use synchronous commit to ensure it completes before function returns
	slog.Info("Calling synchronous CommitOffsetsSync...")

	var commitError error
	f.client.CommitOffsetsSync(ctx, offsetsToCommit, func(client *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		commitError = err // Capture error from callback
		if err != nil {
			slog.Error("Sync commit callback failed", slog.Any("err", err))
		} else {
			slog.Info("Sync commit callback succeeded",
				slog.Int("numTopics", len(resp.Topics)))
			// Log details of committed offsets
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					slog.Info("Committed offset for partition",
						slog.String("topic", topic.Topic),
						slog.Int("partition", int(partition.Partition)),
						slog.Any("errorCode", partition.ErrorCode))
				}
			}
		}
	})

	if commitError != nil {
		slog.Error("Commit failed via callback", slog.Any("err", commitError))
		return commitError
	}

	slog.Info("CommitOffsetsSync completed")

	// Immediately verify the commit worked by checking committed offsets
	committedOffsets := f.client.CommittedOffsets()
	slog.Info("CommitOffsets completed successfully",
		slog.Any("committedOffsets", committedOffsets))

	return nil
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
		kgo.DisableAutoCommit(),
		// Set session timeout for consumer group heartbeats
		kgo.SessionTimeout(10*time.Second),
		// Set heartbeat interval
		kgo.HeartbeatInterval(3*time.Second),
		// Ensure we allow time for rebalancing
		kgo.RebalanceTimeout(30*time.Second),
		// Consumer group lifecycle callbacks with detailed logging
		kgo.OnPartitionsAssigned(func(ctx context.Context, c *kgo.Client, assigned map[string][]int32) {
			for topic, partitions := range assigned {
				slog.Info("🎉 Partitions assigned",
					slog.String("topic", topic),
					slog.Any("partitions", partitions),
					slog.String("groupID", cfg.Kafka.GroupID))
				// Check group metadata during assignment for debugging
				actualGroupID, generation := c.GroupMetadata()
				slog.Info("Group metadata during assignment",
					slog.String("actualGroupID", actualGroupID),
					slog.Int("generation", int(generation)))
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
	slog.Info("Creating franz-go client with restored configuration",
		slog.String("configuredGroupID", cfg.Kafka.GroupID))
	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		logger.Panic("Failed to create Kafka client", slog.Any("err", err))
	}
	defer client.Close()

	// Immediately check what group ID the client is using
	actualGroupID, generation := client.GroupMetadata()
	slog.Info("Franz-go client created",
		slog.String("actualGroupID", actualGroupID),
		slog.Int("generation", int(generation)),
		slog.String("configuredGroupID", cfg.Kafka.GroupID))

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

	// Wait for consumer group coordination to complete
	// This is crucial - we need to give franz-go time to join the consumer group
	slog.Info("Waiting for consumer group coordination...")

	// Brief wait to allow partition assignment to complete
	time.Sleep(2 * time.Second)

	// Check consumer group status after initialization
	groupID, generation := client.GroupMetadata()
	slog.Info("Consumer group status after initialization",
		slog.String("groupID", groupID),
		slog.Int("generation", int(generation)))

	connectCount := 0
	// Single consumer loop for all topics
	for {
		// Check if we're properly joined to the consumer group before polling
		groupID, generation := client.GroupMetadata()
		if groupID == "" || generation < 0 {
			slog.Info("⏳ Consumer group not ready, waiting...",
				slog.String("groupID", groupID),
				slog.Int("generation", int(generation)),
				slog.Any("brokers", client.DiscoveredBrokers()),
			)
			time.Sleep(2 * time.Second)
			connectCount++
			if connectCount >= 5 {
				logger.Fatal(fmt.Sprintf("Consumer group not ready after %d attempts, exiting... Check if TLS needs to be enabled/disabled", connectCount), slog.String("groupID", groupID), slog.Int("generation", int(generation)), slog.Any("brokers", client.DiscoveredBrokers()))
			}
			continue
		} else {
			connectCount = 0
		}

		kafkaMsg, err := kafkaConsumer.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context cancelled, exit gracefully
				slog.Info("Consumer context cancelled")
				return
			}
			slog.Debug("No kafka message available, continuing polling", slog.Any("err", err))

			// Check if consumer is still assigned to partitions
			groupID, generation := client.GroupMetadata()
			slog.Debug("Consumer group status during read error",
				slog.String("groupID", groupID),
				slog.Int("generation", int(generation)))

			time.Sleep(500 * time.Millisecond)
			continue
		}

		if kafkaMsg == nil {
			// No message available after timeout, continue polling
			slog.Debug("No message available, continuing polling")
			continue
		}

		slog.Info("✅ Successfully read message",
			slog.String("topic", kafkaMsg.Topic),
			slog.Int("partition", int(kafkaMsg.Partition)),
			slog.Int64("offset", kafkaMsg.Offset))

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
