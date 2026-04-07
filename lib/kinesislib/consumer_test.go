package kinesislib

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
)

type mockKinesisClient struct {
	listShardsFunc       func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error)
	getShardIteratorFunc func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	getRecordsFunc       func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
}

func (m *mockKinesisClient) ListShards(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
	if m.listShardsFunc != nil {
		return m.listShardsFunc(ctx, params, optFns...)
	}
	return nil, errors.New("ListShards not implemented")
}

func (m *mockKinesisClient) GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
	if m.getShardIteratorFunc != nil {
		return m.getShardIteratorFunc(ctx, params, optFns...)
	}
	return nil, errors.New("GetShardIterator not implemented")
}

func (m *mockKinesisClient) GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
	if m.getRecordsFunc != nil {
		return m.getRecordsFunc(ctx, params, optFns...)
	}
	return nil, errors.New("GetRecords not implemented")
}

func TestKinesisMessage(t *testing.T) {
	now := time.Now()
	msg := KinesisMessage{
		topic:       "test-stream",
		partition:   2,
		offset:      100,
		key:         []byte("test-key"),
		value:       []byte("test-value"),
		highWater:   500,
		publishTime: now,
	}

	assert.Equal(t, "test-stream", msg.Topic())
	assert.Equal(t, 2, msg.Partition())
	assert.Equal(t, int64(100), msg.Offset())
	assert.Equal(t, []byte("test-key"), msg.Key())
	assert.Equal(t, []byte("test-value"), msg.Value())
	assert.Equal(t, int64(500), msg.HighWaterMark())
	assert.Equal(t, now, msg.PublishTime())
}

func TestShardIndexMap(t *testing.T) {
	mockClient := &mockKinesisClient{
		listShardsFunc: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{ShardId: aws.String("shardId-000000000000")},
					{ShardId: aws.String("shardId-000000000001")},
					{ShardId: aws.String("shardId-000000000002")},
				},
			}, nil
		},
		getShardIteratorFunc: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("iter-" + aws.ToString(params.ShardId)),
			}, nil
		},
	}

	consumer, err := NewConsumer(context.Background(), mockClient, "test-stream")
	assert.NoError(t, err)

	assert.Equal(t, 0, consumer.shardIndexMap["shardId-000000000000"])
	assert.Equal(t, 1, consumer.shardIndexMap["shardId-000000000001"])
	assert.Equal(t, 2, consumer.shardIndexMap["shardId-000000000002"])

	// Test stable on repeat lookup
	assert.Equal(t, 1, consumer.shardIndexMap["shardId-000000000001"])
}

func TestFetchMessage_EmptyRecords(t *testing.T) {
	callCount := 0
	mockClient := &mockKinesisClient{
		listShardsFunc: func(ctx context.Context, params *kinesis.ListShardsInput, optFns ...func(*kinesis.Options)) (*kinesis.ListShardsOutput, error) {
			return &kinesis.ListShardsOutput{
				Shards: []types.Shard{
					{ShardId: aws.String("shardId-000000000000")},
				},
			}, nil
		},
		getShardIteratorFunc: func(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("mock-iterator"),
			}, nil
		},
		getRecordsFunc: func(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error) {
			callCount++
			if callCount == 1 {
				// First call returns empty records, which should trigger a sleep and continue
				return &kinesis.GetRecordsOutput{
					Records:            []types.Record{}, // empty
					NextShardIterator:  aws.String("mock-iterator-2"),
					MillisBehindLatest: aws.Int64(0),
				}, nil
			}
			// Second call returns records
			return &kinesis.GetRecordsOutput{
				Records: []types.Record{
					{
						Data:                        []byte("test-data"),
						PartitionKey:                aws.String("test-key"),
						ApproximateArrivalTimestamp: aws.Time(time.Now()),
					},
				},
				NextShardIterator:  aws.String("mock-iterator-3"),
				MillisBehindLatest: aws.Int64(10),
			}, nil
		},
	}

	consumer, err := NewConsumer(context.Background(), mockClient, "test-stream")
	assert.NoError(t, err)

	start := time.Now()

	msg, err := consumer.FetchMessage(context.Background())
	assert.NoError(t, err)

	duration := time.Since(start)

	// Since we sleep for 1 second on empty records, the duration should be roughly >= 1s
	assert.True(t, duration >= time.Second, "Expected to sleep for at least 1s on empty GetRecords response")
	assert.Equal(t, 2, callCount)
	assert.Equal(t, []byte("test-data"), msg.Value())
}

func TestCommitMessages(t *testing.T) {
	consumer := &Consumer{
		streamName:    "test-stream",
		shardIndexMap: map[string]int{"shardId-0000": 0},
	}

	msg1 := KinesisMessage{topic: "test-stream", partition: 0, offset: 1}
	msg2 := KinesisMessage{topic: "test-stream", partition: 0, offset: 2}

	// CommitMessages is basically a no-op that just logs, but it shouldn't error.
	err := consumer.CommitMessages(context.Background(), msg1, msg2)
	assert.NoError(t, err)
}
