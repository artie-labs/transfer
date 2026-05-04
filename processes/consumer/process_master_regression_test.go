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
	"github.com/artie-labs/transfer/models"
)

// processMessageMetricCount returns the number of Timing calls recorded against
// the "process.message" metric — one is expected per processed message.
func processMessageMetricCount(m *mocks.FakeMetricsClient) int {
	n := 0
	for i := 0; i < m.TimingCallCount(); i++ {
		name, _, _ := m.TimingArgsForCall(i)
		if name == "process.message" {
			n++
		}
	}
	return n
}

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
	return buildMongoMessageWithID(t, op, 1004)
}

func buildMongoMessageWithID(t *testing.T, op string, id int) artie.Message {
	t.Helper()
	doc := fmt.Sprintf(`{\"_id\": \"%d\",\"first_name\": \"Anne\"}`, id)
	body := fmt.Sprintf(`"after": "%s", "before": null`, doc)
	if op == "d" {
		body = fmt.Sprintf(`"before": "%s", "after": null`, doc)
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
		Key:   []byte(fmt.Sprintf("Struct{id=%d}", id)),
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
	m := &mocks.FakeMetricsClient{}

	args := processArgs{
		Msg:                    buildMongoMessage(t, "r"),
		GroupID:                "g",
		TopicToConfigFormatMap: regressionTcFmtMap(regressionTC("")),
	}

	gotID, err := args.process(t.Context(), cfg, memDB, dest, m)
	assert.NoError(t, err)
	assert.Equal(t, tableID, gotID)
	assert.Equal(t, 0, dest.MergeCallCount(), "no flush expected when buffer is not full")
	assert.Equal(t, 0, dest.AppendCallCount())
	assert.Equal(t, uint(1), memDB.GetOrCreateTableData(tableID, regressionTopic).NumberOfRows())
	assert.Equal(t, 1, processMessageMetricCount(m))
}

func TestProcess_SingleMessage_FlushesWhenBufferFull(t *testing.T) {
	// BufferRows=0 → ShouldFlush returns true after a single insert.
	// Mode must be set so the flush path can dispatch to merge vs. append.
	cfg := config.Config{Mode: config.Replication, BufferRows: 0, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}
	dest.MergeReturns(true, nil)
	m := &mocks.FakeMetricsClient{}
	ctx, fc := withConsumerCtx(t)

	args := processArgs{
		Msg:                    buildMongoMessage(t, "r"),
		GroupID:                "g",
		TopicToConfigFormatMap: regressionTcFmtMap(regressionTC("")),
	}

	gotID, err := args.process(ctx, cfg, memDB, dest, m)
	assert.NoError(t, err)
	assert.Equal(t, tableID, gotID)
	assert.Equal(t, 1, dest.MergeCallCount())
	assert.Equal(t, 1, fc.CommitMessagesCallCount())
	assert.Equal(t, 1, processMessageMetricCount(m))
}

func TestProcess_SingleMessage_SkipDoesNotFlush(t *testing.T) {
	cfg := config.Config{BufferRows: 10, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}
	m := &mocks.FakeMetricsClient{}

	args := processArgs{
		Msg:                    buildMongoMessage(t, "d"),
		GroupID:                "g",
		TopicToConfigFormatMap: regressionTcFmtMap(regressionTC("d")),
	}

	gotID, err := args.process(t.Context(), cfg, memDB, dest, m)
	assert.NoError(t, err)
	assert.Equal(t, tableID, gotID)
	assert.Equal(t, 0, dest.MergeCallCount())
	assert.Equal(t, uint(0), memDB.GetOrCreateTableData(tableID, regressionTopic).NumberOfRows())
	assert.Equal(t, 1, processMessageMetricCount(m), "process.message must still be emitted on skip")
}

func TestProcess_SingleMessage_UnmarshalErrorDoesNotFlush(t *testing.T) {
	cfg := config.Config{BufferRows: 10, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}
	m := &mocks.FakeMetricsClient{}

	bad := artie.NewFranzGoMessage(kgo.Record{
		Topic: regressionTopic, Key: []byte("Struct{id=1}"), Value: []byte("not json"),
		Timestamp: time.Time{},
	}, 0)
	args := processArgs{
		Msg:                    bad,
		GroupID:                "g",
		TopicToConfigFormatMap: regressionTcFmtMap(regressionTC("")),
	}

	_, err := args.process(t.Context(), cfg, memDB, dest, m)
	assert.Error(t, err)
	assert.Equal(t, 0, dest.MergeCallCount())
	assert.Equal(t, 1, processMessageMetricCount(m), "process.message must still be emitted on error")
}

// --- Multi-message regression cases ---
//
// On master, processArgs.process handles a single message at a time and is
// driven in a loop by FetchMessageAndProcess. These tests simulate that loop
// to pin behavior across N messages: total rows, total flushes, and total
// process.message timings. After the per-topic-batching change collapses
// the loop into a single args.process call with []artie.Message, the same
// totals must still hold — invariants don't depend on whether the messages
// are processed one-by-one or as a slice.

func processN(t *testing.T, msgs []artie.Message, cfg config.Config, tcFmt *TcFmtMap, memDB *models.DatabaseData, dest *mocks.FakeDestination, m *mocks.FakeMetricsClient, ctx context.Context) []error {
	t.Helper()
	errs := make([]error, len(msgs))
	for i, msg := range msgs {
		args := processArgs{
			Msg:                    msg,
			GroupID:                "g",
			TopicToConfigFormatMap: tcFmt,
		}
		_, errs[i] = args.process(ctx, cfg, memDB, dest, m)
	}
	return errs
}

func TestProcess_Multiple_NoneFlush(t *testing.T) {
	cfg := config.Config{Mode: config.Replication, BufferRows: 10, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}
	m := &mocks.FakeMetricsClient{}
	tcFmt := regressionTcFmtMap(regressionTC(""))

	msgs := []artie.Message{
		buildMongoMessageWithID(t, "r", 1),
		buildMongoMessageWithID(t, "r", 2),
		buildMongoMessageWithID(t, "r", 3),
	}
	errs := processN(t, msgs, cfg, tcFmt, memDB, dest, m, t.Context())
	for i, err := range errs {
		assert.NoError(t, err, "message %d", i)
	}

	assert.Equal(t, 0, dest.MergeCallCount(), "no flush expected when buffer is not full")
	assert.Equal(t, uint(3), memDB.GetOrCreateTableData(tableID, regressionTopic).NumberOfRows())
	assert.Equal(t, 3, processMessageMetricCount(m))
}

func TestProcess_Multiple_LastTriggersFlush(t *testing.T) {
	// BufferRows=2 → 3rd InsertRow makes NumberOfRows() > 2, triggering flush.
	cfg := config.Config{Mode: config.Replication, BufferRows: 2, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}
	dest.MergeReturns(true, nil)
	m := &mocks.FakeMetricsClient{}
	ctx, fc := withConsumerCtx(t)
	tcFmt := regressionTcFmtMap(regressionTC(""))

	msgs := []artie.Message{
		buildMongoMessageWithID(t, "r", 1),
		buildMongoMessageWithID(t, "r", 2),
		buildMongoMessageWithID(t, "r", 3),
	}
	errs := processN(t, msgs, cfg, tcFmt, memDB, dest, m, ctx)
	for i, err := range errs {
		assert.NoError(t, err, "message %d", i)
	}

	// Only the third triggers shouldFlush, so exactly one Merge.
	assert.Equal(t, 1, dest.MergeCallCount())
	assert.Equal(t, 1, fc.CommitMessagesCallCount())
	assert.Equal(t, 3, processMessageMetricCount(m))
}

func TestProcess_Multiple_OneSkipped(t *testing.T) {
	cfg := config.Config{Mode: config.Replication, BufferRows: 10, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}
	m := &mocks.FakeMetricsClient{}
	tcFmt := regressionTcFmtMap(regressionTC("d")) // skip deletes

	msgs := []artie.Message{
		buildMongoMessageWithID(t, "r", 1),
		buildMongoMessageWithID(t, "d", 2), // skipped
		buildMongoMessageWithID(t, "r", 3),
	}
	errs := processN(t, msgs, cfg, tcFmt, memDB, dest, m, t.Context())
	for i, err := range errs {
		assert.NoError(t, err, "message %d", i)
	}

	assert.Equal(t, 0, dest.MergeCallCount())
	// Skipped row is not inserted, so 2 rows total.
	assert.Equal(t, uint(2), memDB.GetOrCreateTableData(tableID, regressionTopic).NumberOfRows())
	// process.message is still emitted for the skipped message.
	assert.Equal(t, 3, processMessageMetricCount(m))
}

func TestProcess_Multiple_OneErrorsOthersStillProcess(t *testing.T) {
	cfg := config.Config{Mode: config.Replication, BufferRows: 10, FlushSizeKb: 900, FlushIntervalSeconds: 10}
	memDB := models.NewMemoryDB()
	dest := &mocks.FakeDestination{}
	m := &mocks.FakeMetricsClient{}
	tcFmt := regressionTcFmtMap(regressionTC(""))

	bad := artie.NewFranzGoMessage(kgo.Record{
		Topic: regressionTopic, Key: []byte("Struct{id=2}"), Value: []byte("not json"),
	}, 0)
	msgs := []artie.Message{
		buildMongoMessageWithID(t, "r", 1),
		bad,
		buildMongoMessageWithID(t, "r", 3),
	}
	errs := processN(t, msgs, cfg, tcFmt, memDB, dest, m, t.Context())
	assert.NoError(t, errs[0])
	assert.Error(t, errs[1])
	assert.NoError(t, errs[2])

	assert.Equal(t, 0, dest.MergeCallCount())
	// Only the two valid messages produced rows.
	assert.Equal(t, uint(2), memDB.GetOrCreateTableData(tableID, regressionTopic).NumberOfRows())
	// process.message is emitted for every attempt, including the failed one.
	assert.Equal(t, 3, processMessageMetricCount(m))
}
