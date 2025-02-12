package consumer

import (
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/artie"
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
	mockEvent.GetTableNameReturns("foo")

	for i := 0; i < 5; i++ {
		mockEvent.GetDataReturns(map[string]any{
			"id":                                fmt.Sprintf("pk-%d", i),
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"abc":                               "def",
			"hi":                                "hello",
		}, nil)

		evt, err := event.ToMemoryEvent(mockEvent, map[string]any{"id": fmt.Sprintf("pk-%d", i)}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(f.T(), err)

		kafkaMsg := kafka.Message{Partition: 1, Offset: 1}

		_, _, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
		assert.Nil(f.T(), err)

		td := f.db.GetOrCreateTableData("foo")
		assert.Equal(f.T(), int(td.NumberOfRows()), i+1)
	}

	assert.Equal(f.T(), uint(5), f.db.GetOrCreateTableData("foo").NumberOfRows())
}

func (f *FlushTestSuite) TestShouldFlush() {
	var flush bool
	var flushReason string

	for i := 0; i < int(float64(f.cfg.BufferRows)*1.5); i++ {
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
		flush, flushReason, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
		assert.Nil(f.T(), err)

		if flush {
			break
		}
	}

	assert.Equal(f.T(), "rows", flushReason)
	assert.True(f.T(), flush, "Flush successfully triggered via pool size.")
}

func (f *FlushTestSuite) TestMemoryConcurrency() {
	tableNames := []string{"dusty", "snowflake", "postgres"}
	var wg sync.WaitGroup

	// Inserted a bunch of data
	for idx := range tableNames {
		wg.Add(1)
		go func(tableName string) {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				mockEvent := &mocks.FakeEvent{}
				mockEvent.GetTableNameReturns(tableName)
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
				_, _, err = evt.Save(f.cfg, f.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
				assert.Nil(f.T(), err)
			}
		}(tableNames[idx])
	}

	wg.Wait()

	// Verify all the tables exist.
	for idx := range tableNames {
		td := f.db.GetOrCreateTableData(tableNames[idx])
		assert.Len(f.T(), td.Rows(), 5)
	}

	assert.Nil(f.T(), Flush(f.T().Context(), f.db, f.dest, metrics.NullMetricsProvider{}, Args{}), "flush failed")
	assert.Equal(f.T(), f.fakeConsumer.CommitMessagesCallCount(), len(tableNames)) // Commit 3 times because 3 topics.

	for i := 0; i < len(tableNames); i++ {
		_, kafkaMessages := f.fakeConsumer.CommitMessagesArgsForCall(i)
		assert.Equal(f.T(), len(kafkaMessages), 1) // There's only 1 partition right now

		// Within each partition, the offset should be 4 (i < 5 from above).
		assert.Equal(f.T(), kafkaMessages[0].Offset, int64(4))
	}
}
