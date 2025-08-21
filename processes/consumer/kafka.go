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
	"github.com/artie-labs/transfer/lib/jitter"
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
	fetches := f.client.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		return nil, errs[0].Err
	}

	iter := fetches.RecordIter()
	for !iter.Done() {
		return iter.Next(), nil
	}

	return nil, nil // No messages available
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

	var wg sync.WaitGroup
	for num, topic := range topics {
		// It is recommended to not try to establish a connection all at the same time, which may overwhelm the Kafka cluster.
		time.Sleep(jitter.Jitter(100, 3000, num))
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()

			// Create client options for this specific topic
			topicOpts := append(clientOpts,
				kgo.ConsumerGroup(cfg.Kafka.GroupID),
				kgo.ConsumeTopics(topic),
				kgo.OnPartitionsRevoked(func(ctx context.Context, c *kgo.Client, _ map[string][]int32) {
					slog.Debug("Partitions revoked for topic", slog.String("topic", topic))
				}),
				kgo.OnPartitionsLost(func(ctx context.Context, c *kgo.Client, _ map[string][]int32) {
					slog.Warn("Partitions lost for topic", slog.String("topic", topic))
				}),
			)

			client, err := kgo.NewClient(topicOpts...)
			if err != nil {
				logger.Panic("Failed to create Kafka client", slog.Any("err", err), slog.String("topic", topic))
			}

			kafkaConsumer := &FranzGoConsumer{
				client:  client,
				groupID: cfg.Kafka.GroupID,
			}
			topicToConsumer.Add(topic, kafkaConsumer)

			for {
				kafkaMsg, err := kafkaConsumer.ReadMessage(ctx)
				if err != nil {
					slog.Warn("Failed to read kafka message", slog.Any("err", err), slog.String("topic", topic))
					time.Sleep(500 * time.Millisecond)
					continue
				}

				if kafkaMsg == nil {
					// No message available, continue polling
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

				// Commit the message manually
				if err := kafkaConsumer.CommitMessages(ctx, kafkaMsg); err != nil {
					slog.Warn("Failed to commit message", slog.Any("err", err))
				}
			}
		}(topic)
	}

	wg.Wait()
}
