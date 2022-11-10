package kafka

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/artie-labs/transfer/lib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/models"
)

var kafkaConsumer kafkalib.Consumer

// SetKafkaConsumer - This is used for tests.
func SetKafkaConsumer(consumer kafkalib.Consumer) {
	kafkaConsumer = consumer
}

func CommitOffset(topic string, partitionsToOffset map[int32]string) error {
	var topicPartitions []kafka.TopicPartition
	for partition, offset := range partitionsToOffset {
		offsetNum, castErr := strconv.Atoi(offset)
		if castErr != nil {
			return castErr
		}

		topicPartitions = append(topicPartitions, kafka.TopicPartition{
			Topic:     ptr.ToString(topic),
			Partition: partition,
			Offset:    kafka.Offset(offsetNum),
		})
	}

	_, err := kafkaConsumer.CommitOffsets(topicPartitions)
	return err
}

func StartConsumer(ctx context.Context, flushChan chan bool) {
	log := logger.FromContext(ctx)
	log.Info("Starting Kafka consumer...", config.GetSettings().Config.Kafka)
	var err error
	kafkaConsumer, err = kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.GetSettings().Config.Kafka.BootstrapServer,
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"group.id":          config.GetSettings().Config.Kafka.GroupID,
		"sasl.username":     config.GetSettings().Config.Kafka.Username,
		"sasl.password":     config.GetSettings().Config.Kafka.Password,
		// IMPORTANT: We only commit the offset when Flush is successful.
		"enable.auto.commit": false,
	})

	if err != nil {
		log.Fatalf("Failed to create consumer client, err: %v", err)
	}

	defer kafkaConsumer.Close()
	topicToConfigMap := make(map[string]kafkalib.TopicConfig)
	var topics []string
	for _, topicConfig := range config.GetSettings().Config.Kafka.TopicConfigs {
		topicToConfigMap[topicConfig.Topic] = topicConfig
		topics = append(topics, topicConfig.Topic)
	}

	err = kafkaConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topics, err: %v", err)
	}

	for {
		// TODO: Break this out into a separate function & test.
		msg, err := kafkaConsumer.ReadMessage(-1)
		if msg == nil {
			log.Info("Msg is nil, skipping...")
			continue
		}

		logFields := map[string]interface{}{
			"topicPartition": msg.TopicPartition.String(),
			"key":            string(msg.Key),
		}

		if err != nil {
			log.WithError(err).WithFields(logFields).Warn("failed to read kafka message")
			continue
		}

		logFields["value"] = string(msg.Value)
		if err == nil {
			pkName, pkValue, err := lib.GetPrimaryKey(ctx, msg.Key)
			if err != nil {
				log.WithError(err).WithFields(logFields).Warn("cannot unmarshall key")
				continue
			}

			var event lib.Event
			err = json.Unmarshal(msg.Value, &event)
			if err != nil {
				// A tombstone event will be sent to Kafka when a DELETE happens.
				// Which causes marshalling error.
				log.WithFields(logFields).WithError(err).Warn("cannot unmarshall event")
				continue
			} else {
				topicConfig, isOk := topicToConfigMap[*msg.TopicPartition.Topic]
				if !isOk {
					log.WithFields(logFields).Warn("Failed to get topic Name")
					continue
				}

				evt := models.ToMemoryEvent(event, pkName, pkValue, topicConfig)
				var shouldFlush bool
				shouldFlush, err = evt.Save(&topicConfig, msg.TopicPartition.Partition, msg.TopicPartition.Offset.String())
				if err != nil {
					log.WithFields(logFields).WithError(err).Error("Event failed to save")
				}

				if shouldFlush {
					flushChan <- true
				}
			}
		} else {
			log.Fatalf("Consumer error: %v", err)
		}
	}
}
