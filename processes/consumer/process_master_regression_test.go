package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc/mongo"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
)

// These tests pin the externally-observable behavior of processArgs.process for
// a single message: when a flush happens, when it doesn't, and what's left in
// memory. They exist so that future changes to the processing path (e.g.
// per-topic batching) have to keep single-message behavior intact.

const regressionTopic = "foo"

func regressionTC(skipOps string) kafkalib.TopicConfig {
	return kafkalib.TopicConfig{
		Database:          testDB,
		TableName:         table,
		Schema:            schema,
		Topic:             regressionTopic,
		CDCKeyFormat:      "org.apache.kafka.connect.storage.StringConverter",
		SkippedOperations: skipOps,
	}
}

func regressionTcFmtMap(tc kafkalib.TopicConfig) *TcFmtMap {
	var mgo mongo.Debezium
	m := NewTcFmtMap()
	m.Add(tc.Topic, NewTopicConfigFormatter(tc, &mgo))
	return m
}

func buildMongoMessage(t *testing.T, op string) artie.Message {
	t.Helper()
	body := `"after": "{\"_id\": \"1004\",\"first_name\": \"Anne\"}", "before": null`
	if op == "d" {
		body = `"before": "{\"_id\": \"1004\",\"first_name\": \"Anne\"}", "after": null`
	}
	val := fmt.Sprintf(`{
		"schema": {"type": "struct", "fields": []},
		"payload": {
			%s,
			"source": {"version":"2.0.0.Final","connector":"mongodb","name":"dbserver1","ts_ms":1668753321000,"db":"inventory","collection":"customers"},
			"op": %q,
			"ts_ms": 1668753329387
		}
	}`, body, op)
	return artie.NewFranzGoMessage(kgo.Record{
		Topic: regressionTopic,
		Key:   []byte("Struct{id=1004}"),
		Value: []byte(val),
	}, 0)
}

// withConsumerCtx wires a kafkalib.ConsumerProvider into the context so
// FlushSingleTopic can resolve one (it errors otherwise).
func withConsumerCtx(t *testing.T) (context.Context, *mocks.FakeConsumer) {
	t.Helper()
	fc := &mocks.FakeConsumer{}
	cp := kafkalib.NewConsumerProviderForTest(fc, regressionTopic, "test-group")
	return context.WithValue(t.Context(), kafkalib.BuildContextKey(regressionTopic), cp), fc
}

func TestProcess_SingleMessage_NoFlushWhenBufferNotFull(t *testing.T) {
	cfg := config.Config{BufferRows: 10, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}

	args := processArgs{
		Msg:                    buildMongoMessage(t, "r"),
		GroupID:                "g",
		TopicToConfigFormatMap: regressionTcFmtMap(regressionTC("")),
	}

	gotID, err := args.process(t.Context(), cfg, memDB, dest, metrics.NullMetricsProvider{})
	assert.NoError(t, err)
	assert.Equal(t, tableID, gotID)
	assert.Equal(t, 0, dest.MergeCallCount(), "no flush expected when buffer is not full")
	assert.Equal(t, 0, dest.AppendCallCount())
	assert.Equal(t, uint(1), memDB.GetOrCreateTableData(tableID, regressionTopic).NumberOfRows())
}

func TestProcess_SingleMessage_FlushesWhenBufferFull(t *testing.T) {
	// BufferRows=0 → ShouldFlush returns true after a single insert.
	// Mode must be set so the flush path can dispatch to merge vs. append.
	cfg := config.Config{Mode: config.Replication, BufferRows: 0, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}
	dest.MergeReturns(true, nil)
	ctx, fc := withConsumerCtx(t)

	args := processArgs{
		Msg:                    buildMongoMessage(t, "r"),
		GroupID:                "g",
		TopicToConfigFormatMap: regressionTcFmtMap(regressionTC("")),
	}

	gotID, err := args.process(ctx, cfg, memDB, dest, metrics.NullMetricsProvider{})
	assert.NoError(t, err)
	assert.Equal(t, tableID, gotID)
	assert.Equal(t, 1, dest.MergeCallCount())
	assert.Equal(t, 1, fc.CommitMessagesCallCount())
}

func TestProcess_SingleMessage_SkipDoesNotFlush(t *testing.T) {
	cfg := config.Config{BufferRows: 10, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}

	args := processArgs{
		Msg:                    buildMongoMessage(t, "d"),
		GroupID:                "g",
		TopicToConfigFormatMap: regressionTcFmtMap(regressionTC("d")),
	}

	gotID, err := args.process(t.Context(), cfg, memDB, dest, metrics.NullMetricsProvider{})
	assert.NoError(t, err)
	assert.Equal(t, tableID, gotID)
	assert.Equal(t, 0, dest.MergeCallCount())
	assert.Equal(t, uint(0), memDB.GetOrCreateTableData(tableID, regressionTopic).NumberOfRows())
}

func TestProcess_SingleMessage_UnmarshalErrorDoesNotFlush(t *testing.T) {
	cfg := config.Config{BufferRows: 10, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}

	bad := artie.NewFranzGoMessage(kgo.Record{
		Topic: regressionTopic, Key: []byte("Struct{id=1}"), Value: []byte("not json"),
		Timestamp: time.Time{},
	}, 0)
	args := processArgs{
		Msg:                    bad,
		GroupID:                "g",
		TopicToConfigFormatMap: regressionTcFmtMap(regressionTC("")),
	}

	_, err := args.process(t.Context(), cfg, memDB, dest, metrics.NullMetricsProvider{})
	assert.Error(t, err)
	assert.Equal(t, 0, dest.MergeCallCount())
}
