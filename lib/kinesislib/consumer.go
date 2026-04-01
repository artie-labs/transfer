package kinesislib

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"

	"github.com/artie-labs/transfer/lib/artie"
)

type KinesisMessage struct {
	topic       string
	partition   int
	offset      int64
	key         []byte
	value       []byte
	highWater   int64
	publishTime time.Time
}

func (k KinesisMessage) PublishTime() time.Time { return k.publishTime }
func (k KinesisMessage) Topic() string          { return k.topic }
func (k KinesisMessage) Partition() int         { return k.partition }
func (k KinesisMessage) Offset() int64          { return k.offset }
func (k KinesisMessage) Key() []byte            { return k.key }
func (k KinesisMessage) Value() []byte          { return k.value }
func (k KinesisMessage) HighWaterMark() int64   { return k.highWater }

type kinesisClient interface {
	ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
}

type Consumer struct {
	client     kinesisClient
	streamName string

	mu            sync.Mutex
	shards        []*shardReader
	shardIndexMap map[string]int // used in tests to verify stable partition mapping

	currentShardIdx int
}

type shardReader struct {
	shardID      string
	partitionID  int
	iterator     *string
	offset       int64 // monotonic per-shard counter
	millisBehind int64
	records      []types.Record
}

func NewConsumer(ctx context.Context, client kinesisClient, streamName string) (*Consumer, error) {
	c := &Consumer{
		client:        client,
		streamName:    streamName,
		shardIndexMap: make(map[string]int),
	}

	if err := c.initShards(ctx); err != nil {
		return nil, fmt.Errorf("init shards: %w", err)
	}

	return c, nil
}

func (c *Consumer) initShards(ctx context.Context) error {
	var partition int
	var nextToken *string

	for {
		input := &kinesis.ListShardsInput{}
		if nextToken != nil {
			input.NextToken = nextToken
		} else {
			input.StreamName = aws.String(c.streamName)
		}

		res, err := c.client.ListShards(ctx, input)
		if err != nil {
			return fmt.Errorf("list shards: %w", err)
		}

		for _, shard := range res.Shards {
			shardID := aws.ToString(shard.ShardId)
			c.shardIndexMap[shardID] = partition

			iterRes, err := c.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
				StreamName:        aws.String(c.streamName),
				ShardId:           shard.ShardId,
				ShardIteratorType: types.ShardIteratorTypeLatest,
			})
			if err != nil {
				return fmt.Errorf("get shard iterator %s: %w", shardID, err)
			}

			c.shards = append(c.shards, &shardReader{
				shardID:     shardID,
				partitionID: partition,
				iterator:    iterRes.ShardIterator,
			})

			partition++
		}

		if res.NextToken == nil {
			break
		}
		nextToken = res.NextToken
	}

	if len(c.shards) == 0 {
		return fmt.Errorf("no shards found for stream %s", c.streamName)
	}

	return nil
}

func (c *Consumer) Close() error {
	return nil
}

func (c *Consumer) FetchMessage(ctx context.Context) (artie.Message, error) {
	nilCount := 0
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		c.mu.Lock()
		reader := c.shards[c.currentShardIdx]

		if len(reader.records) > 0 {
			// Pop from queue
			rec := reader.records[0]
			reader.records = reader.records[1:]
			reader.offset++

			msg := KinesisMessage{
				topic:       c.streamName,
				partition:   reader.partitionID,
				offset:      reader.offset,
				key:         []byte(aws.ToString(rec.PartitionKey)),
				value:       rec.Data,
				highWater:   reader.millisBehind,
				publishTime: aws.ToTime(rec.ApproximateArrivalTimestamp),
			}
			c.mu.Unlock()
			return msg, nil
		}

		if reader.iterator == nil {
			// Shard closed, move to next.
			// If all shards are closed, sleep to avoid infinite loop.
			nilCount++
			if nilCount >= len(c.shards) {
				c.mu.Unlock()
				time.Sleep(1 * time.Second)
				nilCount = 0
				continue
			}
			c.currentShardIdx = (c.currentShardIdx + 1) % len(c.shards)
			c.mu.Unlock()
			continue
		}
		nilCount = 0

		// Fetch preparation. We unlock before the network call to avoid blocking other operations.
		iterator := reader.iterator
		shardID := reader.shardID
		currIdx := c.currentShardIdx
		c.mu.Unlock()

		res, err := c.client.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iterator,
		})
		if err != nil {
			return nil, fmt.Errorf("get records shard %s: %w", shardID, err)
		}

		c.mu.Lock()
		reader = c.shards[currIdx]
		reader.iterator = res.NextShardIterator
		reader.millisBehind = aws.ToInt64(res.MillisBehindLatest)

		if len(res.Records) == 0 {
			// Sleep on empty response to avoid throttling. Unlock before sleep.
			c.currentShardIdx = (c.currentShardIdx + 1) % len(c.shards)
			c.mu.Unlock()
			time.Sleep(1 * time.Second)
			continue
		}

		reader.records = res.Records
		c.mu.Unlock()
	}
}

func (c *Consumer) CommitMessages(ctx context.Context, msgs ...artie.Message) error {
	offsets := make(map[int]int64)
	for _, msg := range msgs {
		if msg.Offset() > offsets[msg.Partition()] {
			offsets[msg.Partition()] = msg.Offset()
		}
	}

	slog.Debug("Committed messages to memory",
		slog.String("stream", c.streamName),
		slog.Any("offsets", offsets))

	return nil
}
