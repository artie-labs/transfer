package models

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

var topicConfig = &kafkalib.TopicConfig{
	Database:  "customer",
	TableName: "users",
	Schema:    "public",
}

func (m *ModelsTestSuite) SaveEvent() {
	expectedCol := "rOBiN TaNG"
	expectedLowerCol := "robin tang"

	anotherCol := "DuStY tHE MINI aussie"
	anotherLowerCol := "dusty the mini aussie"

	event := Event{
		Table:           "foo",
		PrimaryKeyValue: "123",
		Data: map[string]interface{}{
			constants.DeleteColumnMarker: true,
			expectedCol:                  "dusty",
			anotherCol:                   13.37,
		},
	}

	_, err := event.Save(topicConfig, kafka.Message{})
	assert.Nil(m.T(), err)

	optimization := GetMemoryDB().TableData["foo"]
	// Check the in-memory DB columns.
	var found int
	for col := range optimization.Columns {
		if col == expectedLowerCol || col == anotherLowerCol {
			found += 1
		}

		if found == 2 {
			break
		}
	}

	assert.Equal(m.T(), 2, found, optimization.Columns)
}
