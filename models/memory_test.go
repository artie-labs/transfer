package models

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
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

	event := Event{
		Table:           "foo",
		PrimaryKeyValue: "123",
		Data: map[string]interface{}{
			config.DeleteColumnMarker: true,
			expectedCol:               "dusty",
		},
	}

	_, err := event.Save(topicConfig, 1, "1")
	assert.Nil(m.T(), err)

	optimization := GetMemoryDB().TableData["foo"]

	// Check the in-memory DB columns.
	var found bool
	for col := range optimization.Columns {
		found = col == expectedLowerCol
		if found {
			break
		}
	}

	assert.True(m.T(), found, optimization.Columns)
}
