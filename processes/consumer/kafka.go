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
)

func StartConsumer(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client) {
	kafkaConn := kafkalib.NewConnection(cfg.Kafka.EnableAWSMSKIAM, cfg.Kafka.DisableTLS, cfg.Kafka.Username, cfg.Kafka.Password, kafkalib.DefaultTimeout)
	slog.Info("Starting Kafka consumer...", slog.Any("config", cfg.Kafka), slog.Any("authMechanism", kafkaConn.Mechanism()))

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
				kafkaConsumer, ok := kafkalib.GetTopicsToConsumerProviderFromContext(ctx)
				if !ok {
					logger.Fatal("Failed to get topics to consumer provider from context")
				}

				kafkaMsg, err := kafkaConsumer.FetchMessage(ctx, topic)
				if err != nil {
					slog.With(artie.BuildLogFields(kafkaMsg)...).Warn("Failed to read kafka message", slog.Any("err", err))
					time.Sleep(500 * time.Millisecond)
					continue
				}

				if len(kafkaMsg.Value) == 0 {
					slog.Debug("Found a tombstone message, skipping...", artie.BuildLogFields(kafkaMsg)...)
					continue
				}

				msg := artie.NewMessage(kafkaMsg)
				args := processArgs{
					Msg:                    msg,
					GroupID:                kafkaConsumer.GroupID(),
					TopicToConfigFormatMap: tcFmtMap,
				}

				tableID, err := args.process(ctx, cfg, inMemDB, dest, metricsClient)
				if err != nil {
					logger.Fatal("Failed to process message", slog.Any("err", err), slog.String("topic", kafkaMsg.Topic))
				}

				msg.EmitIngestionLag(metricsClient, cfg.Mode, kafkaConsumer.GroupID(), tableID.Table)
				msg.EmitRowLag(metricsClient, cfg.Mode, kafkaConsumer.GroupID(), tableID.Table)
			}
		}(topic)
	}

	wg.Wait()
}
