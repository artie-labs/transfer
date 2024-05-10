package consumer

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"sync"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"

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
	slog.Info("Starting Kafka consumer...", slog.Any("config", cfg.Kafka))
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// If using AWS MSK IAM, we expect this to be set in the ENV VAR
	// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION, or the AWS Profile should be called default.)
	if cfg.Kafka.EnableAWSMSKIAM {
		_awsCfg, err := awsCfg.LoadDefaultConfig(ctx)
		if err != nil {
			logger.Panic("Failed to load aws configuration", slog.Any("err", err))
		}

		dialer.SASLMechanism = aws_msk_iam_v2.NewMechanism(_awsCfg)
		dialer.TLS = &tls.Config{}
	}

	// If username or password is set, then let's enable PLAIN.
	// By default, we will support no auth (local testing) and PLAIN SASL.
	if cfg.Kafka.Username != "" {
		dialer.SASLMechanism = plain.Mechanism{
			Username: cfg.Kafka.Username,
			Password: cfg.Kafka.Password,
		}

		dialer.TLS = &tls.Config{}
	}

	tcFmtMap := NewTcFmtMap()
	topicToConsumer = NewTopicToConsumer()
	var topics []string
	for _, topicConfig := range cfg.Kafka.TopicConfigs {
		tcFmtMap.Add(topicConfig.Topic, TopicConfigFormatter{
			tc:     topicConfig,
			Format: format.GetFormatParser(topicConfig.CDCFormat, topicConfig.Topic),
		})
		topics = append(topics, topicConfig.Topic)
	}

	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()

			kafkaCfg := kafka.ReaderConfig{
				GroupID: cfg.Kafka.GroupID,
				Dialer:  dialer,
				Topic:   topic,
				Brokers: cfg.Kafka.BootstrapServers(),
			}

			kafkaConsumer := kafkalib.NewReader(kafkaCfg)
			topicToConsumer.Add(topic, kafkaConsumer)
			for {
				kafkaMsg, err := kafkaConsumer.FetchMessage(ctx)
				if err != nil {
					if errors.Is(err, kafka.RebalanceInProgress) {
						if err = kafkaConsumer.Reload(); err != nil {
							logger.Fatal("Failed to reload kafka consumer", slog.Any("err", err))
						}
					}

					slog.With(artie.KafkaMsgLogFields(kafkaMsg)...).Warn("Failed to read kafka message", slog.Any("err", err))
					time.Sleep(500 * time.Millisecond)
					continue
				}

				if len(kafkaMsg.Value) == 0 {
					slog.Debug("Found a tombstone message, skipping...", artie.KafkaMsgLogFields(kafkaMsg)...)
					continue
				}

				msg := artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic)
				args := processArgs{
					Msg:                    msg,
					GroupID:                kafkaConsumer.Config().GroupID,
					TopicToConfigFormatMap: tcFmtMap,
				}

				tableName, processErr := args.process(ctx, cfg, inMemDB, dest, metricsClient)
				msg.EmitIngestionLag(metricsClient, cfg.Mode, kafkaConsumer.Config().GroupID, tableName)
				msg.EmitRowLag(metricsClient, cfg.Mode, kafkaConsumer.Config().GroupID, tableName)
				if processErr != nil {
					slog.With(artie.KafkaMsgLogFields(kafkaMsg)...).Warn("Skipping message...", slog.Any("err", processErr))
				}
			}
		}(topic)
	}

	wg.Wait()
}
