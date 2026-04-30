package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/artie/metrics"
	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/fn"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/webhooks"
	"github.com/artie-labs/transfer/models"
)

func StartKafkaConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client, whClient *webhooks.Client, cache *lib.KVCache[string]) {
	encryptionKey, err := cfg.SharedDestinationSettings.BuildEncryptionKey(ctx)
	if err != nil {
		whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
			Error: fmt.Sprintf("Failed to build encryption key: %s", err),
		})
		logger.Fatal("Failed to build encryption key", slog.Any("err", err))
	}

	tcFmtMap := NewTcFmtMap()

	var batchTopics []string
	var nonBatchTopics []string
	for _, topicConfig := range cfg.Kafka.TopicConfigs {
		tcFmtMap.Add(topicConfig.Topic, NewTopicConfigFormatter(*topicConfig, format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic)))
	}

	batchTopics = fn.Map(
		fn.Filter(cfg.Kafka.TopicConfigs, func(topicConfig *kafkalib.TopicConfig) bool {
			return topicConfig != nil && topicConfig.FlushOnReceive
		}),
		func(topicConfig *kafkalib.TopicConfig) string {
			return topicConfig.Topic
		},
	)

	nonBatchTopics = fn.Map(
		fn.Filter(cfg.Kafka.TopicConfigs, func(topicConfig *kafkalib.TopicConfig) bool {
			return topicConfig != nil && !topicConfig.FlushOnReceive
		}),
		func(t1 *kafkalib.TopicConfig) string {
			return t1.Topic
		},
	)

	var nonBatchWg sync.WaitGroup
	for num, topic := range nonBatchTopics {
		// It is recommended to not try to establish a connection all at the same time, which may overwhelm the Kafka cluster.
		time.Sleep(jitter.Jitter(100, 3000, num))
		nonBatchWg.Add(1)
		go func(topic string) {
			defer nonBatchWg.Done()
			defer logger.RecoverFatal()
			kafkaConsumer, err := kafkalib.GetConsumerFromContext(ctx, topic)
			if err != nil {
				whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
					Error: fmt.Sprintf("Failed to start Kafka consumer: %s", err),
					Topic: topic,
				})
				logger.Fatal("Failed to get consumer from context", slog.Any("err", err))
			}

			if cfg.Kafka.WaitForTopics {
				if err := kafkaConsumer.WaitForTopic(ctx); err != nil {
					whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
						Error: fmt.Sprintf("Failed waiting for Kafka topic to exist: %s", err),
						Topic: topic,
					})
					logger.Fatal("Failed waiting for topic to exist", slog.Any("err", err), slog.String("topic", topic))
				}
			}

			var fetchRetries int
			for {
				err = kafkaConsumer.FetchMessageAndProcess(ctx, func(msg artie.Message) error {
					if len(msg.Value()) == 0 {
						slog.Debug("Found a tombstone message, skipping...", artie.BuildLogFields(msg)...)
						return nil
					}

					args := processArgs{
						Msgs:                   []artie.Message{msg},
						GroupID:                kafkaConsumer.GetGroupID(),
						TopicToConfigFormatMap: tcFmtMap,
						WhClient:               whClient,
						EncryptionKey:          encryptionKey,
						Cache:                  cache,
						FlushByDefault:         false,
					}

					tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
					if err != nil {
						whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
							Error: fmt.Sprintf("Failed to process message: %s", err),
							Topic: msg.Topic(),
						})
						logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", msg.Topic()))
					}

					metrics.EmitIngestionLag(msg, metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Schema, tableID.Table)
					metrics.EmitRowLag(msg, metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Schema, tableID.Table)

					return nil
				})
				if err != nil {
					_, isFetchErr := kafkalib.AsFetchMessageError(err)
					if isFetchErr && db.IsRetryableError(err, context.DeadlineExceeded, kafkalib.ErrNoMessages) {
						sleepDuration := jitter.Jitter(500, jitter.DefaultMaxMs, fetchRetries)
						time.Sleep(sleepDuration)
						fetchRetries++
						continue
					} else {
						whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
							Error: fmt.Sprintf("Failed to fetch and process message: %s", err),
							Topic: topic,
						})
						logger.Fatal("Failed to fetch and process message", slog.Any("err", err), slog.String("topic", topic))
					}
				}
				fetchRetries = 0
			}
		}(topic)
	}

	var batchWg sync.WaitGroup
	for num, topic := range batchTopics {
		// It is recommended to not try to establish a connection all at the same time, which may overwhelm the Kafka cluster.
		time.Sleep(jitter.Jitter(100, 3000, num))
		batchWg.Add(1)
		go func(topic string) {
			defer batchWg.Done()
			defer logger.RecoverFatal()
			kafkaConsumer, err := kafkalib.GetConsumerFromContext(ctx, topic)
			if err != nil {
				whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
					Error: fmt.Sprintf("Failed to start Kafka consumer: %s", err),
					Topic: topic,
				})
				logger.Fatal("Failed to get consumer from context", slog.Any("err", err))
			}

			if cfg.Kafka.WaitForTopics {
				if err := kafkaConsumer.WaitForTopic(ctx); err != nil {
					whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
						Error: fmt.Sprintf("Failed waiting for Kafka topic to exist: %s", err),
						Topic: topic,
					})
					logger.Fatal("Failed waiting for topic to exist", slog.Any("err", err), slog.String("topic", topic))
				}
			}

			var fetchRetries int
			for {
				err = kafkaConsumer.FetchBatchAndProcess(ctx, func(msgs []artie.Message) error {
					args := processArgs{
						Msgs:                   msgs,
						GroupID:                kafkaConsumer.GetGroupID(),
						TopicToConfigFormatMap: tcFmtMap,
						WhClient:               whClient,
						EncryptionKey:          encryptionKey,
						Cache:                  cache,
						FlushByDefault:         true,
					}

					_, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
					if err != nil {
						whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
							Error: fmt.Sprintf("Failed to process batch: %s", err),
							Topic: msgs[0].Topic(),
						})
						logger.Fatal("Failed to process batch", slog.Any("err", err), slog.String("topic", msgs[0].Topic()))
					}

					return nil
				})
				if err != nil {
					_, isFetchErr := kafkalib.AsFetchMessageError(err)
					if isFetchErr && db.IsRetryableError(err, context.DeadlineExceeded, kafkalib.ErrNoMessages) {
						sleepDuration := jitter.Jitter(500, jitter.DefaultMaxMs, fetchRetries)
						time.Sleep(sleepDuration)
						fetchRetries++
						continue
					} else {
						whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
							Error: fmt.Sprintf("Failed to fetch and process batch: %s", err),
							Topic: topic,
						})
						logger.Fatal("Failed to fetch and process batch", slog.Any("err", err), slog.String("topic", topic))
					}
				}
				fetchRetries = 0
			}
		}(topic)
	}

	batchWg.Wait()
	nonBatchWg.Wait()
}
