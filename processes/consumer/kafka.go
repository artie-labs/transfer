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
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/webhooks"
	"github.com/artie-labs/transfer/models"
)

func StartKafkaConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client, whClient *webhooks.Client) {
	var encryptionKey []byte
	if cfg.SharedDestinationSettings.EncryptionPassphrase != "" {
		var err error
		encryptionKey, err = cryptography.DecodePassphrase(cfg.SharedDestinationSettings.EncryptionPassphrase)
		if err != nil {
			logger.Fatal("Failed to decode encryption passphrase", slog.Any("err", err))
		}
	}

	tcFmtMap := NewTcFmtMap()
	var topics []string
	for _, topicConfig := range cfg.Kafka.TopicConfigs {
		tcFmtMap.Add(topicConfig.Topic, NewTopicConfigFormatter(*topicConfig, format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic)))
		topics = append(topics, topicConfig.Topic)
	}

	var wg sync.WaitGroup
	for num, topic := range topics {
		// It is recommended to not try to establish a connection all at the same time, which may overwhelm the Kafka cluster.
		time.Sleep(jitter.Jitter(100, 3000, num))
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			defer logger.RecoverFatal()
			kafkaConsumer, err := kafkalib.GetConsumerFromContext(ctx, topic)
			if err != nil {
				logger.Fatal("Failed to get consumer from context", slog.Any("err", err))
			}

			if cfg.Kafka.WaitForTopics {
				if err := kafkaConsumer.WaitForTopic(ctx); err != nil {
					logger.Fatal("Failed waiting for topic to exist", slog.Any("err", err), slog.String("topic", topic))
				}
			}

			for {
				err = kafkaConsumer.FetchMessageAndProcess(ctx, func(msg artie.Message) error {
					if len(msg.Value()) == 0 {
						slog.Debug("Found a tombstone message, skipping...", artie.BuildLogFields(msg)...)
						return nil
					}

					args := processArgs{
						Msg:                    msg,
						GroupID:                kafkaConsumer.GetGroupID(),
						TopicToConfigFormatMap: tcFmtMap,
						WhClient:               whClient,
						EncryptionKey:          encryptionKey,
					}

					tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
					if err != nil {
						whClient.SendEvent(ctx, webhooks.UnableToReplicate, webhooks.SendEventArgs{
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
					if fetchErr, ok := kafkalib.IsFetchMessageError(err); ok && db.IsRetryableError(fetchErr.Err, context.DeadlineExceeded) {
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
