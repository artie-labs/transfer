package consumer

import (
	"context"
	"errors"
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
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
	"github.com/artie-labs/transfer/lib/webhooksutil"
	"github.com/artie-labs/transfer/models"
)

func StartKafkaConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client, whClient *webhooksclient.Client) {
	tcFmtMap := NewTcFmtMap()
	var topics []string
	for _, topicConfig := range cfg.Kafka.TopicConfigs {
		tcFmtMap.Add(topicConfig.Topic, NewTopicConfigFormatter(*topicConfig, format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic)))
		topics = append(topics, topicConfig.Topic)
	}

	var wg sync.WaitGroup
	for num, topic := range topics {
		topicPartitionToTsMsMap := make(map[string]int64)
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
						WhClient:               whClient,
					}

					tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient, topicPartitionToTsMsMap)
					if err != nil {
						whClient.SendEvent(ctx, webhooksutil.UnableToReplicate, map[string]any{
							"error":   "Failed to process message",
							"details": err.Error(),
							"topic":   msg.Topic(),
						})
						logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", msg.Topic()))
					}

					metrics.EmitIngestionLag(msg, metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Table)
					metrics.EmitRowLag(msg, metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Table)

					return nil
				})
				if err != nil {
					if fetchErr, ok := kafkalib.IsFetchMessageError(err); ok && errors.Is(fetchErr.Err, context.DeadlineExceeded) {
						slog.Debug("Failed to read kafka message", slog.Any("err", err), slog.String("topic", topic), slog.Duration("timeout", kafkalib.FetchMessageTimeout))
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
