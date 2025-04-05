package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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
		assert.NoError(f.T(), err)

		td := f.db.GetOrCreateTableData("foo")
		assert.Equal(f.T(), int(td.NumberOfRows()), i+1)
	}

	assert.Equal(f.T(), uint(5), f.db.GetOrCreateTableData("foo").NumberOfRows())
}
func (f *FlushTestSuite) TestMemoryConcurrency() {
	// Channel to signal test completion.
	done := make(chan struct{})

	go func() {
		defer close(done)
		tableNames := []string{"dusty", "snowflake", "postgres"}
		var wg sync.WaitGroup

		// Insert a bunch of data concurrently.
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

					evt, err := event.ToMemoryEvent(
						mockEvent,
						map[string]any{"id": fmt.Sprintf("pk-%d", i)},
						kafkalib.TopicConfig{},
						config.Replication,
					)
					assert.NoError(f.T(), err)

					kafkaMsg := kafka.Message{Partition: 1, Offset: int64(i)}
					_, _, err = evt.Save(
						f.cfg,
						f.db,
						topicConfig,
						artie.NewMessage(&kafkaMsg, kafkaMsg.Topic),
					)
					assert.Nil(f.T(), err)
				}
			}(tableNames[idx])
		}

		wg.Wait()

		// Verify all the tables exist.
		for idx := range tableNames {
			td := f.db.GetOrCreateTableData(tableNames[idx])
			assert.Len(f.T(), td.Rows(), 5)

			f.mockDB.ExpectQuery(`DESC TABLE "CUSTOMER"\."PUBLIC"\.".*"`).WillReturnRows(sqlmock.NewRows([]string{"name", "type", "kind", "null?", "default", "primary key", "unique key", "check", "expression", "comment", "policy name"}).
				AddRow("id", "STRING", "COLUMN", "Y", "", "N", "N", "", "", "", "").
				AddRow("foo", "STRING", "COLUMN", "Y", "", "N", "N", "", "", "", "").
				AddRow("cat", "STRING", "COLUMN", "Y", "", "N", "N", "", "", "", "").
				AddRow("__artie_delete", "BOOLEAN", "COLUMN", "Y", "", "N", "N", "", "", "", "").
				AddRow("__artie_only_set_delete", "BOOLEAN", "COLUMN", "Y", "", "N", "N", "", "", "", ""))
		}

		// Flush data and assert commit messages.
		assert.NoError(f.T(), Flush(f.T().Context(), f.db, f.dest, metrics.NullMetricsProvider{}, Args{}), "flush failed")
		assert.Equal(f.T(), f.fakeConsumer.CommitMessagesCallCount(), len(tableNames)) // Expect commit per topic.

		for i := 0; i < len(tableNames); i++ {
			_, kafkaMessages := f.fakeConsumer.CommitMessagesArgsForCall(i)
			assert.Equal(f.T(), len(kafkaMessages), 1) // Only 1 partition is used.

			// The offset should be 4 as we iterate from 0 to 4.
			assert.Equal(f.T(), kafkaMessages[0].Offset, int64(4))
		}
	}()

	// Wait for completion or timeout after 5 seconds.
	select {
	case <-done:
		// Test completed within 5 seconds.
	case <-time.After(5 * time.Second):
		f.T().Fatal("test timed out after 5 seconds")
	}
}
