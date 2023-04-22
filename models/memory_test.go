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
	"strconv"
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
	assert.Equal(m.T(), ext.Date.Type, column.KindDetails.ExtendedTimeDetails.Type)

	column, isOk = inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("json_object_string")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.String, column.KindDetails)

	column, isOk = inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("json_object_no_schema")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.Struct, column.KindDetails)
}

func (m *ModelsTestSuite) TestEvent_SaveColumnsNoData() {
	var cols typing.Columns
	for i := 0; i < 50; i++ {
		cols.AddColumn(typing.Column{Name: fmt.Sprint(i), KindDetails: typing.Invalid})
	}

	evt := Event{
		Table:   "non_existent",
		Columns: &cols,
		Data: map[string]interface{}{
			"1":                          "123",
			constants.DeleteColumnMarker: true,
		},
		PrimaryKeyMap: map[string]interface{}{
			"1": "123",
		},
	}
	kafkaMsg := kafka.Message{}
	_, err := evt.Save(m.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.NoError(m.T(), err)

	var prevKey string
	for _, col := range inMemoryDB.TableData["non_existent"].InMemoryColumns.GetColumns() {
		if col.Name == constants.DeleteColumnMarker {
			continue
		}

		if prevKey == "" {
			prevKey = col.Name
			continue
		}

		currentKeyParsed, err := strconv.Atoi(col.Name)
		assert.NoError(m.T(), err)

		prevKeyParsed, err := strconv.Atoi(prevKey)
		assert.NoError(m.T(), err)

		// Testing ordering.
		assert.True(m.T(), currentKeyParsed > prevKeyParsed, fmt.Sprintf("current key: %v, prevKey: %v", currentKeyParsed, prevKeyParsed))
	}

	// Now let's add more keys.
	evt.Columns.AddColumn(typing.Column{Name: "foo", KindDetails: typing.Invalid})
	var index int
	for idx, col := range evt.Columns.GetColumns() {
		if col.Name == "foo" {
			index = idx
		}
	}

	assert.Equal(m.T(), len(evt.Columns.GetColumns())-1, index, "new column inserted to the end")
}

func (m *ModelsTestSuite) TestEventSaveColumns() {
	var cols typing.Columns
	cols.AddColumn(typing.Column{
		Name:        "randomCol",
		KindDetails: typing.Invalid,
	})
	cols.AddColumn(typing.Column{
		Name:        "anotherCOL",
		KindDetails: typing.Invalid,
	})
	cols.AddColumn(typing.Column{
		Name:        "created_at_date_string",
		KindDetails: typing.Invalid,
	})

	event := Event{
		Table:   "foo",
		Columns: &cols,
		PrimaryKeyMap: map[string]interface{}{
			"id": "123",
		},
		Data: map[string]interface{}{
			constants.DeleteColumnMarker: true,
			"randomCol":                  "dusty",
			"anotherCOL":                 13.37,
			"created_at_date_string":     "2023-01-01",
		},
	}

	kafkaMsg := kafka.Message{}
	_, err := event.Save(m.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(m.T(), err)

	column, isOk := inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("randomcol")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.String, column.KindDetails)

	column, isOk = inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("anothercol")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.Float, column.KindDetails)

	column, isOk = inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn("created_at_date_string")
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), ext.DateKindType, column.KindDetails.ExtendedTimeDetails.Type)

	column, isOk = inMemoryDB.TableData["foo"].InMemoryColumns.GetColumn(constants.DeleteColumnMarker)
	assert.True(m.T(), isOk)
	assert.Equal(m.T(), typing.Boolean, column.KindDetails)
}
