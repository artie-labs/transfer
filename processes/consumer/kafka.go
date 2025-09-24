package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/artie/metrics"
	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/kafkalib/fgo"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
)

func StartKafkaGoConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client) {
	tcFmtMap := NewTcFmtMap()
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
			for {
				kafkaConsumer, err := kafkalib.GetConsumerFromContext(ctx, topic)
				if err != nil {
					logger.Fatal("Failed to get consumer from context", slog.Any("err", err))
				}

				err = kafkaConsumer.FetchMessageAndProcess(ctx, func(msg artie.Message) error {
					if len(msg.Value()) == 0 {
						slog.Debug("Found a tombstone message, skipping...", artie.BuildLogFields(msg)...)
						return nil
					}

					args := processArgs{
						Msg:                    msg,
						GroupID:                kafkaConsumer.GetGroupID(),
						TopicToConfigFormatMap: tcFmtMap,
					}

					tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
					if err != nil {
						logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", msg.Topic()))
					}

					metrics.EmitIngestionLag(msg, metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Table)
					metrics.EmitRowLag(msg, metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Table)

					return nil
				})

				if err != nil {
					if kafkalib.IsFetchMessageError(err) {
						slog.Warn("Failed to read kafka message", slog.Any("err", err))
						time.Sleep(500 * time.Millisecond)
						continue
					} else {
						logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", topic))
					}
				}
			}
		}(topic)
	}

	wg.Wait()
}

func StartFranzGoConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client) {
	tcFmtMap := NewTcFmtMap()
	var topics []string
	for _, topicConfig := range cfg.Kafka.TopicConfigs {
		tcFmtMap.Add(topicConfig.Topic, TopicConfigFormatter{
			tc:     *topicConfig,
			Format: format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic),
		})
		topics = append(topics, topicConfig.Topic)
	}

	// Get the consumer from context (using the first topic since FranzGo uses shared client)
	consumer, err := kafkalib.GetConsumerFromContext(ctx, topics[0])
	if err != nil {
		logger.Fatal("Failed to get consumer from context", slog.Any("err", err))
	}

	// Get the underlying FranzGoConsumer to access the client
	franzGoConsumer := consumer.Consumer.(*fgo.FranzGoConsumer)
	client := franzGoConsumer.Client()

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

		msg, err := consumer.FetchMessage(ctx)
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

		if msg.Topic() == "" {
			// No message available after timeout, continue polling
			slog.Debug("No message available, continuing polling")
			continue
		}

		slog.Info("✅ Successfully read message",
			slog.String("topic", msg.Topic()),
			slog.Int("partition", int(msg.Partition())),
			slog.Int64("offset", msg.Offset()))

		if len(msg.Value()) == 0 {
			slog.Debug("Found a tombstone message, skipping...", artie.BuildLogFields(msg)...)
			continue
		}

		args := processArgs{
			Msg:                    msg,
			GroupID:                consumer.GetGroupID(),
			TopicToConfigFormatMap: tcFmtMap,
		}

		tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
		if err != nil {
			logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", msg.Topic()))
		}

		metrics.EmitIngestionLag(msg, metricsClient, cfg.Mode, consumer.GetGroupID(), tableID.Table)
		metrics.EmitRowLag(msg, metricsClient, cfg.Mode, consumer.GetGroupID(), tableID.Table)
	}
}
