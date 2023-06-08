package consumer

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/artie-labs/transfer/lib/cdc/format"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/segmentio/kafka-go"
)

var topicToConsumer map[string]kafkalib.Consumer

// SetKafkaConsumer - This is used for tests.
func SetKafkaConsumer(_topicToConsumer map[string]kafkalib.Consumer) {
	topicToConsumer = _topicToConsumer
}

func StartConsumer(ctx context.Context) {
	log := logger.FromContext(ctx)
	settings := config.FromContext(ctx)
	log.Info("Starting Kafka consumer...", settings.Config.Kafka)

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	// If using AWS MSK IAM, we expect this to be set in the ENV VAR
	// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or the AWS Profile should be called default.)
	if settings.Config.Kafka.EnableAWSMSKIAM {
		cfg, err := awsCfg.LoadDefaultConfig(ctx)
		if err != nil {
			log.WithError(err).Fatal("failed to load aws configuration")
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

	topicToConfigFmtMap := make(map[string]TopicConfigFormatter)
	topicToConsumer = make(map[string]kafkalib.Consumer)
	var topics []string
	for _, topicConfig := range settings.Config.Kafka.TopicConfigs {
		topicToConfigFmtMap[topicConfig.Topic] = TopicConfigFormatter{
			tc:     topicConfig,
			Format: format.GetFormatParser(ctx, topicConfig.CDCFormat, topicConfig.Topic),
		}
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
			}
			var brokers []string
			for _, bootstrapServer := range strings.Split(settings.Config.Kafka.BootstrapServer, ",") {
				brokers = append(brokers, bootstrapServer)
			}

			kafkaCfg.Brokers = brokers
			kafkaConsumer := kafka.NewReader(kafkaCfg)
			topicToConsumer[topic] = kafkaConsumer
			for {
				kafkaMsg, err := kafkaConsumer.FetchMessage(ctx)
				msg := artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic)
				logFields := map[string]interface{}{
					"topic":  msg.Topic,
					"offset": kafkaMsg.Offset,
					"key":    string(msg.Key()),
					"value":  string(msg.Value()),
				}

				if err != nil {
					log.WithError(err).WithFields(logFields).Warn("failed to read kafka message")
					continue
				}

				msg.EmitIngestionLag(ctx, kafkaConsumer.Config().GroupID)
				processErr := processMessage(ctx, ProcessArgs{
					Msg:                    msg,
					GroupID:                kafkaConsumer.Config().GroupID,
					TopicToConfigFormatMap: topicToConfigFmtMap,
				})
				if processErr != nil {
					log.WithError(processErr).WithFields(logFields).Warn("skipping message...")
				}
			}
		}(topic)
	}

	wg.Wait()
}
