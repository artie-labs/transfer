package consumer

import (
	"context"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func (f *FlushTestSuite) TestFlushSingleTopic_NilDB() {
	assert.NoError(f.T(), FlushSingleTopic(f.T().Context(), nil, f.baseline, metrics.NullMetricsProvider{}, Args{Reason: "test"}, "topic", false))
}

func (f *FlushTestSuite) TestFlushSingleTopic_NoTables() {
	assert.NoError(f.T(), FlushSingleTopic(f.T().Context(), f.db, f.baseline, metrics.NullMetricsProvider{}, Args{Reason: "test"}, "topic", false))
}

func (f *FlushTestSuite) TestFlushSingleTopic_Success() {
	topicName := "test-topic"
	consumer := kafkalib.NewConsumerProviderForTest(f.fakeConsumer, topicName, "test-group")
	ctx := context.WithValue(f.T().Context(), kafkalib.BuildContextKey(topicName), consumer)

	tableID := cdc.NewTableID("public", "users")
	td := f.db.GetOrCreateTableData(tableID, topicName)
	td.SetTableData(optimization.NewTableData(&columns.Columns{}, config.Replication, []string{"id"}, topicConfig, tableID.Table))
	td.InsertRow("1", map[string]any{"id": 1, "name": "Alice"}, false)

	f.fakeBaseline.MergeReturns(true, nil)
	assert.NoError(f.T(), FlushSingleTopic(ctx, f.db, f.baseline, metrics.NullMetricsProvider{}, Args{Reason: "test"}, topicName, false))
	assert.Equal(f.T(), 1, f.fakeBaseline.MergeCallCount())
	assert.Equal(f.T(), 1, f.fakeConsumer.CommitMessagesCallCount())
	assert.True(f.T(), td.Empty())
}

func (f *FlushTestSuite) TestFlushSingleTopic_EmptyTable() {
	topicName := "test-topic"
	consumer := kafkalib.NewConsumerProviderForTest(f.fakeConsumer, topicName, "test-group")
	ctx := context.WithValue(f.T().Context(), kafkalib.BuildContextKey(topicName), consumer)

	tableID := cdc.NewTableID("public", "empty")
	f.db.GetOrCreateTableData(tableID, topicName)

	assert.NoError(f.T(), FlushSingleTopic(ctx, f.db, f.baseline, metrics.NullMetricsProvider{}, Args{Reason: "test"}, topicName, false))
	assert.Equal(f.T(), 0, f.fakeBaseline.MergeCallCount())
	assert.Equal(f.T(), 0, f.fakeConsumer.CommitMessagesCallCount())
}

func (f *FlushTestSuite) TestFlushSingleTopic_MultipleTablesSuccess() {
	topicName := "test-topic"
	consumer := kafkalib.NewConsumerProviderForTest(f.fakeConsumer, topicName, "test-group")
	ctx := context.WithValue(f.T().Context(), kafkalib.BuildContextKey(topicName), consumer)

	tableIDs := []cdc.TableID{
		cdc.NewTableID("public", "users"),
		cdc.NewTableID("public", "orders"),
		cdc.NewTableID("public", "products"),
	}

	for _, tableID := range tableIDs {
		td := f.db.GetOrCreateTableData(tableID, topicName)
		td.SetTableData(optimization.NewTableData(&columns.Columns{}, config.Replication, []string{"id"}, topicConfig, tableID.Table))
		td.InsertRow("1", map[string]any{"id": 1, "data": "test"}, false)
	}

	f.fakeBaseline.MergeReturns(true, nil)
	err := FlushSingleTopic(ctx, f.db, f.baseline, metrics.NullMetricsProvider{}, Args{Reason: "test"}, topicName, false)
	assert.NoError(f.T(), err)
	assert.Equal(f.T(), 3, f.fakeBaseline.MergeCallCount())
	assert.Equal(f.T(), 1, f.fakeConsumer.CommitMessagesCallCount())

	for _, tableID := range tableIDs {
		td := f.db.GetOrCreateTableData(tableID, topicName)
		assert.True(f.T(), td.Empty())
	}
}

func (f *FlushTestSuite) TestFlushSingleTopic_MultipleTablesWithCooldown() {
	topicName := "test-topic"
	consumer := kafkalib.NewConsumerProviderForTest(f.fakeConsumer, topicName, "test-group")
	ctx := context.WithValue(f.T().Context(), kafkalib.BuildContextKey(topicName), consumer)

	tableIDs := []cdc.TableID{
		cdc.NewTableID("public", "users"),
		cdc.NewTableID("public", "orders"),
		cdc.NewTableID("public", "products"),
	}

	for _, tableID := range tableIDs {
		td := f.db.GetOrCreateTableData(tableID, topicName)
		td.SetTableData(optimization.NewTableData(&columns.Columns{}, config.Replication, []string{"id"}, topicConfig, tableID.Table))
		td.InsertRow("1", map[string]any{"id": 1, "data": "test"}, false)
	}

	// Set cooldown on one table by simulating a recent flush
	td := f.db.GetOrCreateTableData(tableIDs[1], topicName)
	td.Wipe()
	td.SetTableData(optimization.NewTableData(&columns.Columns{}, config.Replication, []string{"id"}, topicConfig, tableIDs[1].Table))
	td.InsertRow("1", map[string]any{"id": 1, "data": "test"}, false)

	cooldown := 10 * time.Second
	err := FlushSingleTopic(ctx, f.db, f.baseline, metrics.NullMetricsProvider{}, Args{CoolDown: &cooldown, Reason: "test"}, topicName, false)
	assert.NoError(f.T(), err)

	// No tables should have been flushed
	assert.Equal(f.T(), 0, f.fakeBaseline.MergeCallCount())
	assert.Equal(f.T(), 0, f.fakeConsumer.CommitMessagesCallCount())

	// All tables should still have data
	for _, tableID := range tableIDs {
		td := f.db.GetOrCreateTableData(tableID, topicName)
		assert.False(f.T(), td.Empty())
	}
}

func (f *FlushTestSuite) TestFlushSingleTopic_HistoryMode() {
	topicName := "test-topic"
	consumer := kafkalib.NewConsumerProviderForTest(f.fakeConsumer, topicName, "test-group")
	ctx := context.WithValue(f.T().Context(), kafkalib.BuildContextKey(topicName), consumer)

	tableID := cdc.NewTableID("public", "events")
	td := f.db.GetOrCreateTableData(tableID, topicName)
	td.SetTableData(optimization.NewTableData(&columns.Columns{}, config.History, []string{"id"}, topicConfig, tableID.Table))
	td.InsertRow("1", map[string]any{"id": 1, "event": "login"}, false)

	f.fakeBaseline.AppendReturns(nil)
	err := FlushSingleTopic(ctx, f.db, f.baseline, metrics.NullMetricsProvider{}, Args{Reason: "test"}, topicName, false)
	assert.NoError(f.T(), err)
	assert.Equal(f.T(), 1, f.fakeBaseline.AppendCallCount())
	assert.Equal(f.T(), 0, f.fakeBaseline.MergeCallCount())
	assert.Equal(f.T(), 1, f.fakeConsumer.CommitMessagesCallCount())
	assert.True(f.T(), td.Empty())
}

func (f *FlushTestSuite) TestFlushSingleTopic_MergeNoCommit() {
	topicName := "test-topic"
	consumer := kafkalib.NewConsumerProviderForTest(f.fakeConsumer, topicName, "test-group")
	ctx := context.WithValue(f.T().Context(), kafkalib.BuildContextKey(topicName), consumer)

	tableID := cdc.NewTableID("public", "users")
	td := f.db.GetOrCreateTableData(tableID, topicName)
	td.SetTableData(optimization.NewTableData(&columns.Columns{}, config.Replication, []string{"id"}, topicConfig, tableID.Table))
	td.InsertRow("1", map[string]any{"id": 1, "name": "Alice"}, false)

	// Merge succeeds but returns false (don't commit offset)
	f.fakeBaseline.MergeReturns(false, nil)
	assert.NoError(f.T(), FlushSingleTopic(ctx, f.db, f.baseline, metrics.NullMetricsProvider{}, Args{Reason: "test"}, topicName, false))
	assert.Equal(f.T(), 1, f.fakeBaseline.MergeCallCount())
	assert.Equal(f.T(), 0, f.fakeConsumer.CommitMessagesCallCount())
	assert.False(f.T(), td.Empty())
}
