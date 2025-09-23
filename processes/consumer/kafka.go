package consumer

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
	"github.com/segmentio/kafka-go"
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
				kafkaConsumer, err := kafkalib.GetConsumerFromContext[kafka.Message](ctx, topic)
				if err != nil {
					logger.Fatal("Failed to get consumer from context", slog.Any("err", err))
				}

				err = kafkaConsumer.FetchMessageAndProcess(ctx, func(kafkaMsg kafka.Message) error {
					if len(kafkaMsg.Value) == 0 {
						fields, err := artie.BuildLogFields(kafkaMsg)
						if err != nil {
							logger.Fatal("Failed to build log fields", slog.Any("err", err), slog.String("topic", kafkaMsg.Topic))
						}
						slog.Debug("Found a tombstone message, skipping...", fields...)
						return nil
					}

					msg, err := artie.NewMessage(kafkaMsg)
					if err != nil {
						logger.Fatal("Failed to create message", slog.Any("err", err), slog.String("topic", kafkaMsg.Topic))
					}

					args := processArgs[kafka.Message]{
						Msg:                    msg,
						GroupID:                kafkaConsumer.GetGroupID(),
						TopicToConfigFormatMap: tcFmtMap,
					}

					tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
					if err != nil {
						logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", kafkaMsg.Topic))
					}

					msg.EmitIngestionLag(metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Table)
					msg.EmitRowLag(metricsClient, cfg.Mode, kafkaConsumer.GetGroupID(), tableID.Table)

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
