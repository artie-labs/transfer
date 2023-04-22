package models

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

var topicConfig = &kafkalib.TopicConfig{
	Database:  "customer",
	TableName: "users",
	Schema:    "public",
}

func (m *ModelsTestSuite) TestSaveEvent() {
	expectedCol := "rOBiN TaNG"
	expectedLowerCol := "robin__tang"

	anotherCol := "DuStY tHE MINI aussie"
	anotherLowerCol := "dusty__the__mini__aussie"

	event := Event{
		Table: "foo",
		PrimaryKeyMap: map[string]interface{}{
			"id": "123",
		},
		Data: map[string]interface{}{
			constants.DeleteColumnMarker: true,
			expectedCol:                  "dusty",
			anotherCol:                   13.37,
		},
	}

	kafkaMsg := kafka.Message{}
	_, err := event.Save(m.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(m.T(), err)

	optimization := GetMemoryDB().TableData["foo"]
	// Check the in-memory DB columns.
	var found int
	for _, col := range optimization.InMemoryColumns.GetColumns() {
		if col.Name == expectedLowerCol || col.Name == anotherLowerCol {
			found += 1
		}

		if found == 2 {
			break
		}
	}

	assert.Equal(m.T(), 2, found, optimization.InMemoryColumns)

	badColumn := "other"
	edgeCaseEvent := Event{
		Table: "foo",
		PrimaryKeyMap: map[string]interface{}{
			"id": "12344",
		},
		Data: map[string]interface{}{
			constants.DeleteColumnMarker: true,
			expectedCol:                  "dusty",
			anotherCol:                   13.37,
			badColumn:                    "__debezium_unavailable_value",
		},
	}

	newKafkaMsg := kafka.Message{}
	_, err = edgeCaseEvent.Save(m.ctx, topicConfig, artie.NewMessage(&newKafkaMsg, nil, newKafkaMsg.Topic))
	assert.NoError(m.T(), err)
	inMemoryCol, isOk := GetMemoryDB().TableData["foo"].InMemoryColumns.GetColumn(badColumn)
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.Invalid, inMemoryCol.KindDetails)
}

func (m *ModelsTestSuite) TestEvent_SaveCasing() {
	event := Event{
		Table: "foo",
		PrimaryKeyMap: map[string]interface{}{
			"id": "123",
		},
		Data: map[string]interface{}{
			constants.DeleteColumnMarker: true,
			"randomCol":                  "dusty",
			"anotherCOL":                 13.37,
		},
	}

	kafkaMsg := kafka.Message{}
	_, err := event.Save(m.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(m.T(), err)
	rowData := inMemoryDB.TableData["foo"].RowsData[event.PrimaryKeyValue()]
	expectedColumns := []string{"randomcol", "anothercol"}
	for _, expectedColumn := range expectedColumns {
		_, isOk := rowData[expectedColumn]
		assert.True(m.T(), isOk, fmt.Sprintf("expected col: %s, rowsData: %v", expectedColumn, rowData))
	}

}

func (m *ModelsTestSuite) TestEventSaveOptionalSchema() {
	event := Event{
		Table: "foo",
		PrimaryKeyMap: map[string]interface{}{
			"id": "123",
		},
		Data: map[string]interface{}{
			constants.DeleteColumnMarker: true,
			"randomCol":                  "dusty",
			"anotherCOL":                 13.37,
			"created_at_date_string":     "2023-01-01",
			"created_at_date_no_schema":  "2023-01-01",
			"json_object_string":         `{"foo": "bar"}`,
			"json_object_no_schema":      `{"foo": "bar"}`,
		},
		OptiomalSchema: map[string]typing.KindDetails{
			// Explicitly casting this as a string.
			"created_at_date_string": typing.String,
			"json_object_string":     typing.String,
		},
	}

	kafkaMsg := kafka.Message{}
	_, err := event.Save(m.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(m.T(), err)

	column, isOk := inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("created_at_date_string")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.String, column.KindDetails)

	column, isOk = inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("created_at_date_no_schema")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), ext.Date.Type, column.KindDetails)

	column, isOk = inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("json_object_string")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.String, column.KindDetails)

	column, isOk = inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("json_object_no_schema")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.Struct, column.KindDetails)
}
