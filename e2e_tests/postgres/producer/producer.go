package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/artie-labs/transfer/lib/config"
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

// MessageIterator provides an iterator for reading JSON messages from a file
type MessageIterator struct {
	file    *os.File
	decoder *json.Decoder
	started bool
}

// NewMessageIterator creates a new iterator for the given file
func NewMessageIterator(filePath string) (*MessageIterator, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	decoder := json.NewDecoder(file)

	return &MessageIterator{
		file:    file,
		decoder: decoder,
		started: false,
	}, nil
}

// Next reads the next message from the file
func (mi *MessageIterator) Next() (*DebeziumMessage, error) {
	// If this is the first call, we need to consume the opening bracket of the array
	if !mi.started {
		token, err := mi.decoder.Token()
		if err != nil {
			if err == io.EOF {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to read opening bracket: %w", err)
		}

		// Check if it's a delimiter (opening bracket for array)
		if delim, ok := token.(json.Delim); !ok || delim != '[' {
			return nil, fmt.Errorf("expected opening bracket, got %v", token)
		}
		mi.started = true
	}

	// Check if we've reached the end of the array
	if !mi.decoder.More() {
		return nil, nil // End of array
	}

	var msg DebeziumMessage
	err := mi.decoder.Decode(&msg)
	if err != nil {
		if err == io.EOF {
			return nil, nil // End of file
		}
		return nil, fmt.Errorf("failed to decode message: %w", err)
	}
	return &msg, nil
}

// Close closes the file
func (mi *MessageIterator) Close() error {
	return mi.file.Close()
}

func main() {
	ctx := context.Background()

	settings, err := config.LoadSettings(os.Args, true)
	if err != nil {
		log.Fatal("Failed to initialize config", slog.Any("err", err))
	}

	bootstrapServers := []string{settings.Config.Kafka.BootstrapServer}

	// Define topic mappings
	topicMappings := []TopicMapping{
		{
			FilePath: "testdata/dbserver1.inventory.customers.json",
			Topic:    "dbserver1.inventory.customers",
		},
		{
			FilePath: "testdata/dbserver1.inventory.products-10M.json",
			Topic:    "dbserver1.inventory.products",
		},
	}

	log.Println("🚀 Starting Kafka producer for e2e test data...")

	settingTopics := make(map[string]any)
	for _, t := range settings.Config.Kafka.TopicConfigs {
		settingTopics[t.Topic] = nil
	}

	for _, mapping := range topicMappings {
		if _, ok := settingTopics[mapping.Topic]; !ok {
			log.Fatalf("Topic %s not found in settings. Please add it to the config: %v", mapping.Topic, settingTopics)
		}
	}

	for _, mapping := range topicMappings {

		if err := publishFile(ctx, bootstrapServers, mapping); err != nil {
			log.Fatalf("Failed to publish file %s to topic %s: %v", mapping.FilePath, mapping.Topic, err)
		}
	}

	log.Println("✅ All test data published successfully!")
}

func publishFile(ctx context.Context, bootstrapServers []string, mapping TopicMapping) error {
	log.Printf("📖 Reading file: %s", mapping.FilePath)

	// Create message iterator
	iterator, err := NewMessageIterator(mapping.FilePath)
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iterator.Close()

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

	// Process messages in batches to avoid memory issues
	const batchSize = 1000
	var kafkaMessages []kafka.Message
	messageCount := 0

	for {
		msg, err := iterator.Next()
		if err != nil {
			return fmt.Errorf("failed to read message at count %d: %w", messageCount, err)
		}
		if msg == nil {
			break // End of file
		}

		// Convert the entire Debezium message to bytes
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("failed to marshal message %d: %w", messageCount, err)
		}

		// Extract primary key from payload for the message key
		key, err := extractPrimaryKey(*msg)
		if err != nil {
			return fmt.Errorf("failed to extract primary key from message %d: %w", messageCount, err)
		}

		kafkaMessages = append(kafkaMessages, kafka.Message{
			Key:   key,
			Value: msgBytes,
			Time:  time.Now(),
		})

		messageCount++

		// Process batch when it reaches batchSize or at end of file
		if len(kafkaMessages) >= batchSize {
			log.Printf("📤 Publishing %d messages to topic: %q, message count: %d", len(kafkaMessages), mapping.Topic, messageCount)
			if err := publishBatch(ctx, writer, kafkaMessages, mapping.Topic); err != nil {
				return err
			}
			kafkaMessages = kafkaMessages[:0] // Reset slice but keep capacity
		}
	}

	// Publish remaining messages
	if len(kafkaMessages) > 0 {
		log.Printf("📤 Publishing %d messages to topic: %q, message count: %d", len(kafkaMessages), mapping.Topic, messageCount)
		if err := publishBatch(ctx, writer, kafkaMessages, mapping.Topic); err != nil {
			return err
		}
	}

	log.Printf("✅ Successfully published %d messages to %s", messageCount, mapping.Topic)
	return nil
}

func publishBatch(ctx context.Context, writer *kafka.Writer, messages []kafka.Message, topic string) error {
	// Retry logic for auto-created topics
	var writeErr error
	for attempt := 1; attempt <= 3; attempt++ {
		writeErr = writer.WriteMessages(ctx, messages...)
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

	return nil
}

func extractPrimaryKey(msg DebeziumMessage) ([]byte, error) {
	// Parse the payload to extract primary key
	var payload map[string]any
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		return nil, fmt.Errorf("failed to parse payload: %w", err)
	}

	// Get the "after" section which contains the record data
	after, ok := payload["after"].(map[string]any)
	if !ok || after == nil {
		// For delete operations, use "before" section
		before, ok := payload["before"].(map[string]any)
		if !ok || before == nil {
			return nil, fmt.Errorf("no after or before data in payload")
		}
		after = before
	}

	// Extract the ID field (primary key)
	id, ok := after["id"]
	if !ok {
		return nil, fmt.Errorf("no id field found in record")
	}

	// Create JSON key format: {"id": value}
	keyMap := map[string]any{"id": id}
	return json.Marshal(keyMap)
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
