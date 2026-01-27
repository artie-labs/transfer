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
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

	return &MessageIterator{
		file:    file,
		decoder: json.NewDecoder(file),
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
	if err := mi.decoder.Decode(&msg); err != nil {
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
			FilePath: "testdata/dbserver1.inventory.products.json",
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

	// Create Kafka client
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers...),
	)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.Close()

	// Create topic (Debezium image may have auto-creation disabled)
	if err := createTopic(ctx, client, mapping.Topic); err != nil {
		log.Printf("⚠️  Topic creation failed (might already exist): %v", err)
	}

	// Process messages in batches to avoid memory issues
	const batchSize = 1000
	var kafkaRecords []*kgo.Record
	var messageCount int

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

		kafkaRecords = append(kafkaRecords, &kgo.Record{
			Key:       key,
			Value:     msgBytes,
			Topic:     mapping.Topic,
			Timestamp: time.Now(),
		})

		messageCount++

		// Process batch when it reaches batchSize or at end of file
		if len(kafkaRecords) >= batchSize {
			slog.Info("📤 Publishing messages", slog.Int("count", len(kafkaRecords)), slog.String("topic", mapping.Topic), slog.Int("messageCount", messageCount))
			if err := publishBatch(ctx, client, kafkaRecords); err != nil {
				return err
			}
			kafkaRecords = kafkaRecords[:0] // Reset slice but keep capacity
		}
	}

	// Publish remaining messages
	if len(kafkaRecords) > 0 {
		slog.Info("📤 Publishing messages", slog.Int("count", len(kafkaRecords)), slog.String("topic", mapping.Topic), slog.Int("messageCount", messageCount))
		if err := publishBatch(ctx, client, kafkaRecords); err != nil {
			return err
		}
	}

	slog.Info("✅ Successfully published messages", slog.Int("count", messageCount), slog.String("topic", mapping.Topic))
	return nil
}

func publishBatch(ctx context.Context, client *kgo.Client, records []*kgo.Record) error {
	// Retry logic for auto-created topics
	var writeErr error
	for attempt := 1; attempt <= 3; attempt++ {
		results := client.ProduceSync(ctx, records...)
		writeErr = results.FirstErr()
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

func createTopic(ctx context.Context, client *kgo.Client, topicName string) error {
	log.Printf("🆕 Creating topic: %s", topicName)

	req := kmsg.NewPtrCreateTopicsRequest()
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topicName
	reqTopic.NumPartitions = 1
	reqTopic.ReplicationFactor = 1
	req.Topics = append(req.Topics, reqTopic)

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("failed to send create topics request: %w", err)
	}

	if len(resp.Topics) == 0 {
		return fmt.Errorf("no topics in create topics response")
	}

	topicResp := resp.Topics[0]
	if err := kerr.ErrorForCode(topicResp.ErrorCode); err != nil {
		// Topic might already exist, which is okay
		if err == kerr.TopicAlreadyExists {
			return nil
		}
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}
