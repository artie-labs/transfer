package kafka

import (
	"context"
	"crypto/tls"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/cdc/format"
	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/models"
)

type TopicConfigFormatter struct {
	tc kafkalib.TopicConfig
	cdc.Format
}

var topicToConsumer map[string]kafkalib.Consumer

// SetKafkaConsumer - This is used for tests.
func SetKafkaConsumer(_topicToConsumer map[string]kafkalib.Consumer) {
	topicToConsumer = _topicToConsumer
}

func CommitOffset(ctx context.Context, topic string, partitionsToOffset map[int]kafka.Message) error {
	var err error
	for _, msg := range partitionsToOffset {
		err = topicToConsumer[topic].CommitMessages(ctx, msg)
		if err != nil {
			return err
		}
	}

	return err
}

func StartConsumer(ctx context.Context, flushChan chan bool) {
	log := logger.FromContext(ctx)
	log.Info("Starting Kafka consumer...", config.GetSettings().Config.Kafka)

	mechanism := plain.Mechanism{
		Username: config.GetSettings().Config.Kafka.Username,
		Password: config.GetSettings().Config.Kafka.Password,
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

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
				msg, err := kafkaConsumer.FetchMessage(ctx)
				logFields := map[string]interface{}{
					"topic":  msg.Topic,
					"offset": msg.Offset,
					"key":    string(msg.Key),
					"value":  string(msg.Value),
				}

				if err != nil {
					log.WithError(err).WithFields(logFields).Warn("failed to read kafka message")
					continue
				}

				topicConfig, isOk := topicToConfigFmtMap[msg.Topic]
				if !isOk {
					log.WithFields(logFields).Warn("Failed to get topic Name")
					continue
				}

				pkName, pkValue, err := topicConfig.GetPrimaryKey(ctx, msg.Key)
				if err != nil {
					log.WithError(err).WithFields(logFields).Warn("cannot unmarshall key")
					continue
				}

				event, err := topicConfig.GetEventFromBytes(ctx, msg.Value)
				if err != nil {
					// A tombstone event will be sent to Kafka when a DELETE happens.
					// Which causes marshalling error.
					log.WithFields(logFields).WithError(err).Warn("cannot unmarshall event")
					continue
				}

				evt := models.ToMemoryEvent(event, pkName, pkValue, topicConfig.tc)
				var shouldFlush bool
				shouldFlush, err = evt.Save(&topicConfig.tc, msg)
				if err != nil {
					log.WithFields(logFields).WithError(err).Error("Event failed to save")
				}

				if shouldFlush {
					flushChan <- true
				}
			}
		}(topic)
	}

	wg.Wait()
}
