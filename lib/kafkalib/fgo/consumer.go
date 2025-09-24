package fgo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type FranzGoConsumer struct {
	client       *kgo.Client
	groupID      string
	recordBuffer []*kgo.Record
	bufferIndex  int
	// Map to store high watermarks by topic-partition key
	highWatermarks map[string]int64
}

func NewFranzGoConsumer(client *kgo.Client, groupID string) *FranzGoConsumer {
	return &FranzGoConsumer{
		client:         client,
		groupID:        groupID,
		recordBuffer:   make([]*kgo.Record, 0),
		bufferIndex:    0,
		highWatermarks: make(map[string]int64),
	}
}

func (f FranzGoConsumer) Client() *kgo.Client {
	return f.client
}

func (f *FranzGoConsumer) GetHighWatermark(record kgo.Record) int64 {
	key := fmt.Sprintf("%s-%d", record.Topic, record.Partition)
	if hwm, exists := f.highWatermarks[key]; exists {
		return hwm
	}
	return 0 // Default to 0 if not found
}

func (f *FranzGoConsumer) Close() error {
	f.client.Close()
	return nil
}

func (f *FranzGoConsumer) FetchMessage(ctx context.Context) (artie.Message, error) {
	// First, check if we have buffered records from a previous fetch
	if f.bufferIndex < len(f.recordBuffer) {
		record := f.recordBuffer[f.bufferIndex]
		f.bufferIndex++
		slog.Info("Received message",
			slog.String("topic", record.Topic),
			slog.Int("partition", int(record.Partition)),
			slog.Int64("offset", record.Offset))
		return artie.NewFranzGoMessage(*record, f.GetHighWatermark(*record)), nil
	}

	// Buffer is empty or exhausted, need to poll for new records
	// Poll with a longer timeout to allow for consumer group coordination
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	groupID, generation := f.client.GroupMetadata()
	slog.Debug("Polling topics", slog.Any("topics", f.client.GetConsumeTopics()), slog.String("groupID", groupID), slog.Int("generation", int(generation)))
	fetches := f.client.PollFetches(ctx)
	slog.Debug("done polling", "fetches", fetches, slog.Any("topics", f.client.GetConsumeTopics()), slog.String("groupID", groupID), slog.Int("generation", int(generation)))
	if errs := fetches.Errors(); len(errs) > 0 {
		// Don't log timeouts as warnings, they're normal
		if ctx.Err() != context.DeadlineExceeded {
			slog.Warn("Error polling fetches", slog.Any("err", errs[0].Err))
		}
		return nil, errs[0].Err
	}

	// Clear the buffer and collect all records from this fetch
	f.recordBuffer = f.recordBuffer[:0] // Clear slice but keep capacity
	f.bufferIndex = 0

	// Extract high watermarks from fetch partitions
	fetches.EachTopic(func(topic kgo.FetchTopic) {
		topic.EachPartition(func(partition kgo.FetchPartition) {
			// Store high watermark for this topic-partition combination
			key := fmt.Sprintf("%s-%d", topic.Topic, partition.Partition)
			f.highWatermarks[key] = partition.HighWatermark
		})
	})

	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		f.recordBuffer = append(f.recordBuffer, record)
	}

	// If no records were fetched, return nil (normal timeout case)
	if len(f.recordBuffer) == 0 {
		return nil, nil
	}

	// Return the first record from the newly filled buffer
	record := f.recordBuffer[0]
	f.bufferIndex = 1
	slog.Info("📨 Received message",
		slog.String("topic", record.Topic),
		slog.Int("partition", int(record.Partition)),
		slog.Int64("offset", record.Offset))
	return artie.NewFranzGoMessage(*record, f.GetHighWatermark(*record)), nil
}

func (f *FranzGoConsumer) CommitMessages(ctx context.Context, msgs ...artie.Message) error {
	// franz-go handles auto-commit by default, but we can also commit manually
	slog.Info("Committing messages using explicit offset method", slog.Int("numRecords", len(msgs)))

	// Build explicit offset map - Kafka expects the NEXT offset to read
	offsetsToCommit := make(map[string]map[int32]kgo.EpochOffset)

	for i, msg := range msgs {
		slog.Info("Processing message for commit",
			slog.Int("msgIndex", i),
			slog.String("topic", msg.Topic()),
			slog.Int("partition", int(msg.Partition())),
			slog.Int64("currentOffset", msg.Offset()),
			slog.Int64("commitOffset", msg.Offset()+1)) // Commit next offset

		if offsetsToCommit[msg.Topic()] == nil {
			offsetsToCommit[msg.Topic()] = make(map[int32]kgo.EpochOffset)
		}

		// Kafka expects the next offset to read, so we commit offset + 1
		offsetsToCommit[msg.Topic()][int32(msg.Partition())] = kgo.EpochOffset{
			Epoch:  -1, // Use -1 for unknown epoch (franz-go will handle this)
			Offset: msg.Offset() + 1,
		}
	}

	// Check consumer group status before commit
	groupID, generation := f.client.GroupMetadata()
	slog.Info("Committing explicit offsets",
		slog.String("groupID", groupID),
		slog.Int("generation", int(generation)),
		slog.Any("offsetsToCommit", offsetsToCommit))

	// Use synchronous commit to ensure it completes before function returns
	slog.Info("Calling CommitOffsetsSync...")

	var commitError error
	f.client.CommitOffsetsSync(ctx, offsetsToCommit, func(client *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		commitError = err
		if err != nil {
			slog.Error("Sync commit callback failed", slog.Any("err", err))
		} else {
			slog.Info("Sync commit callback succeeded",
				slog.Int("numTopics", len(resp.Topics)))
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					slog.Info("Committed offset for partition",
						slog.String("topic", topic.Topic),
						slog.Int("partition", int(partition.Partition)),
						slog.Any("errorCode", partition.ErrorCode))
				}
			}
		}
	})

	if commitError != nil {
		slog.Error("Commit failed via callback", slog.Any("err", commitError))
		return commitError
	}

	slog.Info("CommitOffsetsSync completed")

	// Immediately verify the commit worked by checking committed offsets
	committedOffsets := f.client.CommittedOffsets()
	slog.Info("CommitOffsets completed successfully",
		slog.Any("committedOffsets", committedOffsets))

	return nil
}
