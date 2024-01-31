package consumer

import (
	"context"
	"crypto/tls"
	"log/slog"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/logger"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
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

// SetKafkaConsumer - This is used for tests.
func SetKafkaConsumer(_topicToConsumer map[string]kafkalib.Consumer) {
	topicToConsumer = &TopicToConsumer{
		topicToConsumer: _topicToConsumer,
	}
}

func StartConsumer(ctx context.Context) {
	settings := config.FromContext(ctx)
	slog.Info("Starting Kafka consumer...", slog.Any("config", settings.Config.Kafka))

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// If using AWS MSK IAM, we expect this to be set in the ENV VAR
	// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or the AWS Profile should be called default.)
	if settings.Config.Kafka.EnableAWSMSKIAM {
		cfg, err := awsCfg.LoadDefaultConfig(ctx)
		if err != nil {
			logger.Panic("Failed to load aws configuration", slog.Any("err", err))
		}

		dialer.SASLMechanism = aws_msk_iam_v2.NewMechanism(cfg)
		dialer.TLS = &tls.Config{}
	}

	// If username or password is set, then let's enable PLAIN.
	// By default, we will support no auth (local testing) and PLAIN SASL.
	if settings.Config.Kafka.Username != "" {
		dialer.SASLMechanism = plain.Mechanism{
			Username: settings.Config.Kafka.Username,
			Password: settings.Config.Kafka.Password,
		}

		dialer.TLS = &tls.Config{}
	}

	tcFmtMap := NewTcFmtMap()
	topicToConsumer = NewTopicToConsumer()
	var topics []string
	for _, topicConfig := range settings.Config.Kafka.TopicConfigs {
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
				GroupID: settings.Config.Kafka.GroupID,
				Dialer:  dialer,
				Topic:   topic,
				Brokers: settings.Config.Kafka.BootstrapServers(),
			}

			kafkaConsumer := kafka.NewReader(kafkaCfg)
			topicToConsumer.Add(topic, kafkaConsumer)
			for {
				kafkaMsg, err := kafkaConsumer.FetchMessage(ctx)
				if err != nil {
					slog.With(artie.KafkaMsgLogFields(kafkaMsg)...).Warn("Failed to read kafka message", slog.Any("err", err))
					continue
				}

				if len(kafkaMsg.Value) == 0 {
					slog.Info("Found a tombstone message, skipping...", artie.KafkaMsgLogFields(kafkaMsg)...)
					continue
				}

				msg := artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic)
				tableName, processErr := processMessage(ctx, ProcessArgs{
					Msg:                    msg,
					GroupID:                kafkaConsumer.Config().GroupID,
					TopicToConfigFormatMap: tcFmtMap,
				})

				msg.EmitIngestionLag(ctx, kafkaConsumer.Config().GroupID, tableName)
				msg.EmitRowLag(ctx, kafkaConsumer.Config().GroupID, tableName)
				if processErr != nil {
					slog.With(artie.KafkaMsgLogFields(kafkaMsg)...).Warn("Skipping message...", slog.Any("err", processErr))
				}
			}
		}(topic)
	}

	wg.Wait()
}
