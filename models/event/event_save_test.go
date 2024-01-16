package event

import (
	"fmt"
	"strconv"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/artie-labs/transfer/models"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

var topicConfig = &kafkalib.TopicConfig{
	Database:  "customer",
	TableName: "users",
	Schema:    "public",
}

func (e *EventsTestSuite) TestSaveEvent() {
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
	_, _, err := event.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(e.T(), err)

	optimization := models.GetMemoryDB(e.ctx).GetOrCreateTableData("foo")
	// Check the in-memory DB columns.
	var found int
	for _, col := range optimization.ReadOnlyInMemoryCols().GetColumns() {
		if col.RawName() == expectedLowerCol || col.RawName() == anotherLowerCol {
			found += 1
		}

		if found == 2 {
			break
		}
	}

	assert.Equal(e.T(), 2, found, optimization.ReadOnlyInMemoryCols)
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
			badColumn:                    constants.ToastUnavailableValuePlaceholder,
		},
	}

	newKafkaMsg := kafka.Message{}
	_, _, err = edgeCaseEvent.Save(e.ctx, topicConfig, artie.NewMessage(&newKafkaMsg, nil, newKafkaMsg.Topic))
	assert.NoError(e.T(), err)

	td := models.GetMemoryDB(e.ctx).GetOrCreateTableData("foo")
	inMemCol, isOk := td.ReadOnlyInMemoryCols().GetColumn(badColumn)
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), typing.Invalid, inMemCol.KindDetails)
}

func (e *EventsTestSuite) TestEvent_SaveCasing() {
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
	_, _, err := event.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(e.T(), err)

	td := models.GetMemoryDB(e.ctx).GetOrCreateTableData("foo")
	rowData := td.RowsData()[event.PrimaryKeyValue()]
	expectedColumns := []string{"randomcol", "anothercol"}
	for _, expectedColumn := range expectedColumns {
		_, isOk := rowData[expectedColumn]
		assert.True(e.T(), isOk, fmt.Sprintf("expected col: %s, rowsData: %v", expectedColumn, rowData))
	}

}

func (e *EventsTestSuite) TestEventSaveOptionalSchema() {
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
		OptionalSchema: map[string]typing.KindDetails{
			// Explicitly casting this as a string.
			"created_at_date_string": typing.String,
			"json_object_string":     typing.String,
		},
	}

	kafkaMsg := kafka.Message{}
	_, _, err := event.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(e.T(), err)

	td := models.GetMemoryDB(e.ctx).GetOrCreateTableData("foo")
	column, isOk := td.ReadOnlyInMemoryCols().GetColumn("created_at_date_string")
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), typing.String, column.KindDetails)

	column, isOk = td.ReadOnlyInMemoryCols().GetColumn("created_at_date_no_schema")
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), ext.Date.Type, column.KindDetails.ExtendedTimeDetails.Type)

	column, isOk = td.ReadOnlyInMemoryCols().GetColumn("json_object_string")
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), typing.String, column.KindDetails)

	column, isOk = td.ReadOnlyInMemoryCols().GetColumn("json_object_no_schema")
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), typing.Struct, column.KindDetails)
}

func (e *EventsTestSuite) TestEvent_SaveColumnsNoData() {
	var cols columns.Columns
	for i := 0; i < 50; i++ {
		cols.AddColumn(columns.NewColumn(fmt.Sprint(i), typing.Invalid))
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
	_, _, err := evt.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.NoError(e.T(), err)

	td := models.GetMemoryDB(e.ctx).GetOrCreateTableData("non_existent")
	var prevKey string
	for _, col := range td.ReadOnlyInMemoryCols().GetColumns() {
		if col.RawName() == constants.DeleteColumnMarker {
			continue
		}

		if prevKey == "" {
			prevKey = col.RawName()
			continue
		}

		currentKeyParsed, err := strconv.Atoi(col.RawName())
		assert.NoError(e.T(), err)

		prevKeyParsed, err := strconv.Atoi(prevKey)
		assert.NoError(e.T(), err)

		// Testing ordering.
		assert.True(e.T(), currentKeyParsed > prevKeyParsed, fmt.Sprintf("current key: %v, prevKey: %v", currentKeyParsed, prevKeyParsed))
	}

	// Now let's add more keys.
	evt.Columns.AddColumn(columns.NewColumn("foo", typing.Invalid))
	var index int
	for idx, col := range evt.Columns.GetColumns() {
		if col.RawName() == "foo" {
			index = idx
		}
	}

	assert.Equal(e.T(), len(evt.Columns.GetColumns())-1, index, "new column inserted to the end")
}

func (e *EventsTestSuite) TestEventSaveColumns() {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("randomCol", typing.Invalid))
	cols.AddColumn(columns.NewColumn("anotherCOL", typing.Invalid))
	cols.AddColumn(columns.NewColumn("created_at_date_string", typing.Invalid))
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
	_, _, err := event.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(e.T(), err)

	td := models.GetMemoryDB(e.ctx).GetOrCreateTableData("foo")

	column, isOk := td.ReadOnlyInMemoryCols().GetColumn("randomcol")
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), typing.String, column.KindDetails)

	column, isOk = td.ReadOnlyInMemoryCols().GetColumn("anothercol")
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), typing.Float, column.KindDetails)

	column, isOk = td.ReadOnlyInMemoryCols().GetColumn("created_at_date_string")
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), ext.DateKindType, column.KindDetails.ExtendedTimeDetails.Type)

	column, isOk = td.ReadOnlyInMemoryCols().GetColumn(constants.DeleteColumnMarker)
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), typing.Boolean, column.KindDetails)
}

func (e *EventsTestSuite) TestEventSaveTestDeleteFlag() {
	event := Event{
		Table: "foo",
		PrimaryKeyMap: map[string]interface{}{
			"id": "123",
		},
		Data: map[string]interface{}{
			constants.DeleteColumnMarker: true,
		},
		Deleted: true,
	}

	kafkaMsg := kafka.Message{}
	_, _, err := event.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.Nil(e.T(), err)
	assert.False(e.T(), models.GetMemoryDB(e.ctx).GetOrCreateTableData("foo").ContainOtherOperations())

	event.Deleted = false
	_, _, err = event.Save(e.ctx, topicConfig, artie.NewMessage(&kafkaMsg, nil, kafkaMsg.Topic))
	assert.NoError(e.T(), err)
	assert.True(e.T(), models.GetMemoryDB(e.ctx).GetOrCreateTableData("foo").ContainOtherOperations())
}
