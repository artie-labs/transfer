package consumer

import (
	"context"
	"crypto/tls"
	"github.com/artie-labs/transfer/lib/artie"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
	"github.com/segmentio/kafka-go/sasl/plain"
	"sync"
	"time"

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

func StartConsumer(ctx context.Context, flushChan chan bool) {
	log := logger.FromContext(ctx)
	log.Info("Starting Kafka consumer...", config.GetSettings().Config.Kafka)

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	var mech sasl.Mechanism

	// If using AWS MSK IAM, we expect this to be set in the ENV VAR
	// (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or the AWS Profile should be called default.)
	if config.GetSettings().Config.Kafka.EnableAWSMSKIAM {
		cfg, err := awsCfg.LoadDefaultConfig(ctx)
		if err != nil {
			log.WithError(err).Fatal("failed to load aws configuration")
		}

		mech = aws_msk_iam_v2.NewMechanism(cfg)

	}

	// If username or password is set, then let's enable PLAIN.
	// By default, we will support no auth (local testing) and PLAIN SASL.
	if config.GetSettings().Config.Kafka.Username != "" {
		mech = plain.Mechanism{
			Username: config.GetSettings().Config.Kafka.Username,
			Password: config.GetSettings().Config.Kafka.Password,
		}

	}
	dialer.SASLMechanism = mech
	dialer.TLS = &tls.Config{}

	topicToConfigFmtMap := make(map[string]TopicConfigFormatter)
	topicToConsumer = make(map[string]kafkalib.Consumer)
	var topics []string
	for _, topicConfig := range config.GetSettings().Config.Kafka.TopicConfigs {
		topicToConfigFmtMap[topicConfig.Topic] = TopicConfigFormatter{
			tc:     topicConfig,
			Format: format.GetFormatParser(ctx, topicConfig.CDCFormat),
		}
		topics = append(topics, topicConfig.Topic)
	}

	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			kafkaConsumer := kafka.NewReader(kafka.ReaderConfig{
				Brokers: []string{config.GetSettings().Config.Kafka.BootstrapServer},
				GroupID: config.GetSettings().Config.Kafka.GroupID,
				Dialer:  dialer,
				Topic:   topic,
			})

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
				shouldFlush, processErr := processMessage(ctx, msg, topicToConfigFmtMap, kafkaConsumer.Config().GroupID)
				if processErr != nil {
					log.WithError(processErr).WithFields(logFields).Warn("skipping message...")
				}

				if shouldFlush {
					flushChan <- true
				}
			}
		}(topic)
	}

	wg.Wait()
}
