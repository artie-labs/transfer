package flush

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"sync"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/models"
)

var topicConfig = &kafkalib.TopicConfig{
	Database:  "customer",
	TableName: "users",
	Schema:    "public",
	Topic:     "foo",
}

func (f *FlushTestSuite) TestMemoryBasic() {
	for i := 0; i < 5; i++ {
		event := models.Event{
			Table:           "foo",
			PrimaryKeyName:  "id",
			PrimaryKeyValue: fmt.Sprintf("pk-%d", i),
			Data: map[string]interface{}{
				config.DeleteColumnMarker: true,
				"abc":                     "def",
				"hi":                      "hello",
			},
		}
		_, err := event.Save(topicConfig, kafka.Message{Partition: 1, Offset: 1})
		assert.Nil(f.T(), err)
		assert.Equal(f.T(), len(models.GetMemoryDB().TableData["foo"].RowsData), i+1)
	}
}

func (f *FlushTestSuite) TestShouldFlush() {
	var flush bool
	for i := 0; i < config.SnowflakeArraySize*1.5; i++ {
		event := models.Event{
			Table:           "postgres",
			PrimaryKeyName:  "id",
			PrimaryKeyValue: fmt.Sprintf("pk-%d", i),
			Data: map[string]interface{}{
				config.DeleteColumnMarker: true,
				"pk":                      fmt.Sprintf("pk-%d", i),
				"foo":                     "bar",
				"cat":                     "dog",
			},
		}

		var err error
		flush, err = event.Save(topicConfig, kafka.Message{Partition: 1, Offset: int64(i)})
		assert.Nil(f.T(), err)

		if flush {
			break
		}
	}

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
				event := models.Event{
					Table:           tableName,
					PrimaryKeyName:  "id",
					PrimaryKeyValue: fmt.Sprintf("pk-%d", i),
					Data: map[string]interface{}{
						config.DeleteColumnMarker: true,
						"pk":                      fmt.Sprintf("pk-%d", i),
						"foo":                     "bar",
						"cat":                     "dog",
					},
				}

				_, err := event.Save(topicConfig, kafka.Message{Partition: 1, Offset: int64(i)})
				assert.Nil(f.T(), err)
			}

		}(tableNames[idx])
	}

	wg.Wait()

	// Verify all the tables exist.
	for idx := range tableNames {
		tableConfig := models.GetMemoryDB().TableData[tableNames[idx]].RowsData
		assert.Equal(f.T(), len(tableConfig), 5)
	}

	assert.Nil(f.T(), Flush(context.Background()), "flush failed")
	assert.Equal(f.T(), f.fakeConsumer.CommitMessagesCallCount(), len(tableNames)) // Commit 3 times because 3 topics.

	for i := 0; i < len(tableNames); i++ {
		_, kafkaMessages := f.fakeConsumer.CommitMessagesArgsForCall(i)
		assert.Equal(f.T(), len(kafkaMessages), 1) // There's only 1 partition right now

		// Within each partition, the offset should be 4 (i < 5 from above).
		assert.Equal(f.T(), kafkaMessages[0].Offset, int64(4))
	}

}
