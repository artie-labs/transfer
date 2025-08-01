package consumer

import (
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models/event"
)

var topicConfig = kafkalib.TopicConfig{
	Database:  "customer",
	TableName: "users",
	Schema:    "public",
	Topic:     "foo",
}

func (f *FlushTestSuite) TestMemoryBasic() {
	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns(topicConfig.TableName)
	expectedTableID := cdc.NewTableID(topicConfig.Schema, topicConfig.TableName)

	for i := range 5 {
		mockEvent.GetDataReturns(map[string]any{
			"id":                                fmt.Sprintf("pk-%d", i),
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"abc":                               "def",
			"hi":                                "hello",
		}, nil)

		evt, err := event.ToMemoryEvent(mockEvent, map[string]any{"id": fmt.Sprintf("pk-%d", i)}, topicConfig, config.Replication)
		assert.NoError(f.T(), err)

		kafkaMsg := kafka.Message{Partition: 1, Offset: 1}
		_, _, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(kafkaMsg))
		assert.NoError(f.T(), err)

		td := f.db.GetOrCreateTableData(expectedTableID)
		assert.Equal(f.T(), int(td.NumberOfRows()), i+1)
	}

	assert.Equal(f.T(), f.db.GetOrCreateTableData(expectedTableID).NumberOfRows(), uint(5))
}

func (f *FlushTestSuite) TestShouldFlush() {
	var flush bool
	var flushReason string

	for i := range int(float64(f.cfg.BufferRows) * 1.5) {
		mockEvent := &mocks.FakeEvent{}
		mockEvent.GetTableNameReturns("postgres")
		mockEvent.GetDataReturns(map[string]any{
			"id":                                fmt.Sprintf("pk-%d", i),
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"pk":                                fmt.Sprintf("pk-%d", i),
			"foo":                               "bar",
			"cat":                               "dog",
		}, nil)

		evt, err := event.ToMemoryEvent(mockEvent, map[string]any{"id": fmt.Sprintf("pk-%d", i)}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(f.T(), err)

		kafkaMsg := kafka.Message{Partition: 1, Offset: int64(i)}
		flush, flushReason, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(kafkaMsg))
		assert.NoError(f.T(), err)

		if flush {
			break
		}
	}

	assert.Equal(f.T(), "rows", flushReason)
	assert.True(f.T(), flush, "Flush successfully triggered via pool size.")
}

func (f *FlushTestSuite) TestMemoryConcurrency() {
	tableIDs := []cdc.TableID{
		cdc.NewTableID("public", "dusty"),
		cdc.NewTableID("public", "snowflake"),
		cdc.NewTableID("public", "postgres"),
	}
	var wg sync.WaitGroup

	// Inserted a bunch of data
	for idx := range tableIDs {
		wg.Add(1)
		go func(tableID cdc.TableID) {
			defer wg.Done()
			for i := range 5 {
				mockEvent := &mocks.FakeEvent{}
				mockEvent.GetTableNameReturns(tableID.Table)
				mockEvent.GetDataReturns(map[string]any{
					"id":                                fmt.Sprintf("pk-%d", i),
					constants.DeleteColumnMarker:        true,
					constants.OnlySetDeleteColumnMarker: true,
					"pk":                                fmt.Sprintf("pk-%d", i),
					"foo":                               "bar",
					"cat":                               "dog",
				}, nil)

				evt, err := event.ToMemoryEvent(mockEvent, map[string]any{"id": fmt.Sprintf("pk-%d", i)}, kafkalib.TopicConfig{Schema: tableID.Schema}, config.Replication)
				assert.NoError(f.T(), err)

				kafkaMsg := kafka.Message{Partition: 1, Offset: int64(i)}
				_, _, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(kafkaMsg))
				assert.NoError(f.T(), err)
			}
		}(tableIDs[idx])
	}

	wg.Wait()

	// Verify all the tables exist.
	for idx := range tableIDs {
		td := f.db.GetOrCreateTableData(tableIDs[idx])
		assert.Len(f.T(), td.Rows(), 5)
	}

	f.fakeBaseline.MergeReturns(true, nil)

	// Inject HWM context for the test
	ctx := kafkalib.InjectHWMIntoContext(f.T().Context())
	assert.NoError(f.T(), Flush(ctx, f.db, f.baseline, metrics.NullMetricsProvider{}, Args{}))
	assert.Equal(f.T(), f.fakeConsumer.CommitMessagesCallCount(), len(tableIDs)) // Commit 3 times because 3 topics.

	for i := range len(tableIDs) {
		_, kafkaMessages := f.fakeConsumer.CommitMessagesArgsForCall(i)
		assert.Equal(f.T(), len(kafkaMessages), 1) // There's only 1 partition right now

		// Within each partition, the offset should be 4 (i < 5 from above).
		assert.Equal(f.T(), kafkaMessages[0].Offset, int64(4))
	}
}
