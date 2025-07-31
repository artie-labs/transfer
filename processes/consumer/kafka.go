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

var topicToConsumer *TopicToConsumer

func NewTopicToConsumer() *TopicToConsumer {
	return &TopicToConsumer{
		topicToConsumer: make(map[string]kafkalib.Consumer),
	}
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

	dialer, err := kafkaConn.Dialer(ctx)
	if err != nil {
		logger.Panic("Failed to create Kafka dialer", slog.Any("err", err))
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

			kafkaCfg := kafka.ReaderConfig{
				GroupID:     cfg.Kafka.GroupID,
				Dialer:      dialer,
				Topic:       topic,
				Brokers:     cfg.Kafka.BootstrapServers(true),
				MaxAttempts: 1,
			}

			kafkaConsumer := kafka.NewReader(kafkaCfg)
			topicToConsumer.Add(topic, kafkaConsumer)
			for {
				kafkaMsg, err := kafkaConsumer.FetchMessage(ctx)
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

				hwm, err := kafkalib.GetHWMFromContext(ctx)
				if err != nil {
					logger.Fatal("Failed to get HWM from context", slog.Any("err", err))
				}

				if hwm, ok := hwm.GetHWM(kafkaMsg.Topic, kafkaMsg.Partition); ok {
					// Should never happen
					if hwm > kafkaMsg.Offset {
						// Skip this message since we already processed it.
						// continue
						logger.Panic("Found a message with an offset that is less than the high watermark",
							slog.String("topic", kafkaMsg.Topic),
							slog.Int("partition", kafkaMsg.Partition),
							slog.Int64("hwm", hwm),
							slog.Int64("offset", kafkaMsg.Offset),
						)
					}
				}

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
		}(topic)
	}

	wg.Wait()
}
