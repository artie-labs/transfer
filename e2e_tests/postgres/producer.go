package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

// DebeziumMessage represents the structure of messages in the test data files
type DebeziumMessage struct {
	Schema  json.RawMessage `json:"schema"`
	Payload json.RawMessage `json:"payload"`
}

// TopicMapping defines which file goes to which topic
type TopicMapping struct {
	FilePath string
	Topic    string
}

func main() {
	ctx := context.Background()
	bootstrapServers := []string{"localhost:29092"}

	// Define topic mappings
	topicMappings := []TopicMapping{
		{
			FilePath: "testdata/dbserver1.inventory.customers.json",
			Topic:    "dbserver1.inventory.customers",
		},
		{
			FilePath: "testdata/dbserver1.inventory.products.json",
			Topic:    "dbserver1.inventory.products",
		},
	}

	log.Println("🚀 Starting Kafka producer for e2e test data...")

	for _, mapping := range topicMappings {
		if err := publishFile(ctx, bootstrapServers, mapping); err != nil {
			log.Fatalf("Failed to publish file %s to topic %s: %v", mapping.FilePath, mapping.Topic, err)
		}
	}

	log.Println("✅ All test data published successfully!")
}

func publishFile(ctx context.Context, bootstrapServers []string, mapping TopicMapping) error {
	log.Printf("📖 Reading file: %s", mapping.FilePath)

	// Read and parse the JSON file
	fileContent, err := os.ReadFile(mapping.FilePath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	var messages []DebeziumMessage
	if err := json.Unmarshal(fileContent, &messages); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	log.Printf("📝 Found %d messages in %s", len(messages), mapping.FilePath)

	// Create Kafka writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: bootstrapServers,
		Topic:   mapping.Topic,
	})
	defer writer.Close()

	// Create topic (Debezium image may have auto-creation disabled)
	if err := createTopic(ctx, bootstrapServers, mapping.Topic); err != nil {
		log.Printf("⚠️  Topic creation failed (might already exist): %v", err)
	}

	// Publish each message
	kafkaMessages := make([]kafka.Message, 0, len(messages))
	for i, msg := range messages {
		// Convert the entire Debezium message to bytes
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message %d: %w", i, err)
		}

		kafkaMessages = append(kafkaMessages, kafka.Message{
			Key:   nil, // Let Kafka assign partition
			Value: msgBytes,
			Time:  time.Now(),
		})
	}

	log.Printf("📤 Publishing %d messages to topic: %s", len(kafkaMessages), mapping.Topic)

	// Retry logic for auto-created topics
	var writeErr error
	for attempt := 1; attempt <= 3; attempt++ {
		writeErr = writer.WriteMessages(ctx, kafkaMessages...)
		if writeErr == nil {
			break
		}

		if attempt < 3 {
			log.Printf("⚠️  Attempt %d failed, retrying in 2s: %v", attempt, writeErr)
			time.Sleep(2 * time.Second)
		}
	}

	if writeErr != nil {
		return fmt.Errorf("failed to write messages after 3 attempts: %w", writeErr)
	}

	log.Printf("✅ Successfully published %d messages to %s", len(kafkaMessages), mapping.Topic)
	return nil
}

func createTopic(ctx context.Context, bootstrapServers []string, topicName string) error {
	conn, err := kafka.DialContext(ctx, "tcp", bootstrapServers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to kafka: %w", err)
	}
	defer conn.Close()

	log.Printf("🆕 Creating topic: %s", topicName)
	return conn.CreateTopics(kafka.TopicConfig{
		Topic:             topicName,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
}
