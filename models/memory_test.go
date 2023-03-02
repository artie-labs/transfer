package models

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
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
	for col := range optimization.InMemoryColumns {
		if col == expectedLowerCol || col == anotherLowerCol {
			found += 1
		}

		if found == 2 {
			break
		}
	}

	assert.Equal(m.T(), 2, found, optimization.InMemoryColumns)

	badColumn := "other"
	edgeCaseEvent := Event{
		Table:           "foo",
		PrimaryKeyValue: "12344",
		Data: map[string]interface{}{
			constants.DeleteColumnMarker: true,
			expectedCol:                  "dusty",
			anotherCol:                   13.37,
			badColumn:                    "__debezium_unavailable_value",
		},
	}

	_, err = edgeCaseEvent.Save(topicConfig, kafka.Message{})
	assert.NoError(m.T(), err)
	val, isOk := GetMemoryDB().TableData["foo"].InMemoryColumns[badColumn]
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), val, typing.Invalid)
}
