package fgo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type FranzGoConsumer struct {
	client  *kgo.Client
	groupID string
	topic   string
	// Map to store high watermarks by topic-partition key
	highWatermarks map[string]int64
	currentIter    *kgo.FetchesRecordIter
}

func GetHighWatermarkMapKey(topic string, partition int32) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func NewFranzGoConsumer(client *kgo.Client, groupID string, topic string) *FranzGoConsumer {
	return &FranzGoConsumer{
		client:         client,
		groupID:        groupID,
		topic:          topic,
		highWatermarks: make(map[string]int64),
	}
}

func (f FranzGoConsumer) Client() *kgo.Client {
	return f.client
}

func (f *FranzGoConsumer) GetHighWatermark(record kgo.Record) int64 {
	if hwm, exists := f.highWatermarks[GetHighWatermarkMapKey(record.Topic, record.Partition)]; exists {
		return hwm
	}
	return 0 // Default to 0 if not found
}

func (f *FranzGoConsumer) Close() error {
	f.client.Close()
	return nil
}

func (f *FranzGoConsumer) FetchMessage(ctx context.Context) (artie.Message, error) {
	if f.currentIter != nil && !f.currentIter.Done() {
		record := f.currentIter.Next()
		slog.Debug("📨 Received message",
			slog.String("topic", record.Topic),
			slog.Int("partition", int(record.Partition)),
			slog.Int64("offset", record.Offset))

		return artie.NewFranzGoMessage(*record, f.GetHighWatermark(*record)), nil
	}

	groupID, generation := f.client.GroupMetadata()
	slog.Debug("Polling topics", slog.Any("topics", f.client.GetConsumeTopics()), slog.String("groupID", groupID), slog.Int("generation", int(generation)))

	fetches := f.client.PollFetches(ctx)
	slog.Debug("done polling", "fetches", fetches, slog.Any("topics", f.client.GetConsumeTopics()), slog.String("groupID", groupID), slog.Int("generation", int(generation)))

	if errs := fetches.Errors(); len(errs) > 0 {
		var combinedErrors []error
		for _, err := range errs {
			combinedErrors = append(combinedErrors, err.Err)
		}
		return nil, errors.Join(combinedErrors...)
	}

	// Since HWM is a field on the Partition and not on every kgo.Record,
	// we need to iterate over the partitions and update the high watermark map.
	fetches.EachTopic(func(topic kgo.FetchTopic) {
		topic.EachPartition(func(partition kgo.FetchPartition) {
			f.highWatermarks[GetHighWatermarkMapKey(topic.Topic, partition.Partition)] = partition.HighWatermark
		})
	})

	f.currentIter = fetches.RecordIter()
	if f.currentIter.Done() {
		return nil, nil
	}

	record := f.currentIter.Next()

	return artie.NewFranzGoMessage(*record, f.GetHighWatermark(*record)), nil
}

func (f *FranzGoConsumer) CommitMessages(ctx context.Context, msgs ...artie.Message) error {
	offsetsToCommit := make(map[string]map[int32]kgo.EpochOffset)

	for i, msg := range msgs {
		slog.Debug("Processing message for commit",
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

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		// Check consumer group status before commit
		groupID, generation := f.client.GroupMetadata()
		slog.Debug("Committing explicit offsets",
			slog.String("groupID", groupID),
			slog.Int("generation", int(generation)),
			slog.Any("offsetsToCommit", offsetsToCommit))
	}

	var commitError error
	f.client.CommitOffsetsSync(ctx, offsetsToCommit, func(client *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		commitError = err
		if err != nil {
			slog.Error("Sync commit callback failed", slog.Any("err", err))
		} else {
			if slog.Default().Enabled(ctx, slog.LevelDebug) {
				slog.Debug("Sync commit callback succeeded",
					slog.Int("numTopics", len(resp.Topics)))
				for _, topic := range resp.Topics {
					for _, partition := range topic.Partitions {
						slog.Debug("Committed offset for partition",
							slog.String("topic", topic.Topic),
							slog.Int("partition", int(partition.Partition)),
							slog.Any("errorCode", partition.ErrorCode))
					}
				}
			}
		}
	})

	if commitError != nil {
		return fmt.Errorf("commit failed via callback: %w", commitError)
	}

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		committedOffsets := f.client.CommittedOffsets()
		slog.Debug("CommitOffsets completed successfully",
			slog.Any("committedOffsets", committedOffsets))
	}

	return nil
}
