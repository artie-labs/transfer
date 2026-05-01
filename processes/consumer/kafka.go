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
			processTopic(topic, &nonBatchWg, ctx, whClient, cfg, tcFmtMap, encryptionKey, inMemDB, dest, metricsClient, false)
		}(topic)
	}

	var batchWg sync.WaitGroup
	for num, topic := range batchTopics {
		// It is recommended to not try to establish a connection all at the same time, which may overwhelm the Kafka cluster.
		time.Sleep(jitter.Jitter(100, 3000, num))
		batchWg.Add(1)
		go func(topic string) {
			processTopic(topic, &batchWg, ctx, whClient, cfg, tcFmtMap, encryptionKey, inMemDB, dest, metricsClient, true)
		}(topic)
	}

	batchWg.Wait()
	nonBatchWg.Wait()
}

func processTopic(topic string, wg *sync.WaitGroup, ctx context.Context, whClient *webhooks.Client, cfg config.Config, tcFmtMap *TcFmtMap, encryptionKey []byte, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client, isBatch bool) {
	defer wg.Done()
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
		msgOrBatch := ""
		if isBatch {
			msgOrBatch = "batch"
			err = kafkaConsumer.FetchBatchAndProcess(ctx, func(msg []artie.Message) error {
				return processMessages(msg, kafkaConsumer, tcFmtMap, whClient, encryptionKey, ctx, cfg, inMemDB, dest, metricsClient, isBatch)
			})
		} else {
			msgOrBatch = "message"
			err = kafkaConsumer.FetchMessageAndProcess(ctx, func(msg []artie.Message) error {
				return processMessages(msg, kafkaConsumer, tcFmtMap, whClient, encryptionKey, ctx, cfg, inMemDB, dest, metricsClient, isBatch)
			})
		}
		if err != nil {
			_, isFetchErr := kafkalib.AsFetchMessageError(err)
			if isFetchErr && db.IsRetryableError(err, context.DeadlineExceeded, kafkalib.ErrNoMessages) {
				sleepDuration := jitter.Jitter(500, jitter.DefaultMaxMs, fetchRetries)
				time.Sleep(sleepDuration)
				fetchRetries++
				continue
			} else {
				whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
					Error: fmt.Sprintf("Failed to fetch and process %s: %s", msgOrBatch, err),
					Topic: topic,
				})
				logger.Fatal(fmt.Sprintf("Failed to fetch and process %s", msgOrBatch), slog.Any("err", err), slog.String("topic", topic))
			}
		}
		fetchRetries = 0
	}
}

func processMessages(msgs []artie.Message, kafkaConsumer *kafkalib.ConsumerProvider, tcFmtMap *TcFmtMap, whClient *webhooks.Client, encryptionKey []byte, ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client, flushByDefault bool) error {
	tombstoneMsgs := fn.Filter(msgs, func(msg artie.Message) bool {
		return len(msg.Value()) == 0
	})
	nonTombstoneMsgs := fn.Filter(msgs, func(msg artie.Message) bool {
		return len(msg.Value()) != 0
	})
	for _, msg := range tombstoneMsgs {
		slog.Debug("Found a tombstone message, skipping...", artie.BuildLogFields(msg)...)
	}

	args := processArgs{
		Msgs:                   nonTombstoneMsgs,
		GroupID:                kafkaConsumer.GetGroupID(),
		TopicToConfigFormatMap: tcFmtMap,
		WhClient:               whClient,
		EncryptionKey:          encryptionKey,
		FlushByDefault:         flushByDefault,
	}

	tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
	if err != nil {
		topic := ""
		for _, msg := range msgs {
			topic = msg.Topic()
			whClient.SendEvent(ctx, webhooks.EventReplicationError, webhooks.EventProperties{
				Error: fmt.Sprintf("Failed to process message: %s", err),
				Topic: msg.Topic(),
			})
		}
		logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", topic))
	}

	for _, msg := range msgs {
		metrics.EmitIngestionLag(msg, metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Schema, tableID.Table)
		metrics.EmitRowLag(msg, metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Schema, tableID.Table)
	}

	return nil
}
