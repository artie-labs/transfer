package event

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

var topicConfig = kafkalib.TopicConfig{
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
		Table:       "foo",
		primaryKeys: []string{"id"},
		Data: map[string]any{
			"id":                                "123",
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			expectedCol:                         "dusty",
			anotherCol:                          13.37,
		},
	}

	kafkaMsg := kafka.Message{}
	_, _, err := event.Save(e.cfg, e.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
	assert.NoError(e.T(), err)

	optimization := e.db.GetOrCreateTableData("foo")
	// Check the in-memory DB columns.
	var found int
	for _, col := range optimization.ReadOnlyInMemoryCols().GetColumns() {
		if col.Name() == expectedLowerCol || col.Name() == anotherLowerCol {
			found += 1
		}

		if found == 2 {
			break
		}
	}

	assert.Equal(e.T(), 2, found, optimization.ReadOnlyInMemoryCols)
	badColumn := "other"
	edgeCaseEvent := Event{
		Table:       "foo",
		primaryKeys: []string{"id"},
		Data: map[string]any{
			"id":                                "12344",
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			expectedCol:                         "dusty",
			anotherCol:                          13.37,
			badColumn:                           constants.ToastUnavailableValuePlaceholder,
		},
	}

	newKafkaMsg := kafka.Message{}
	_, _, err = edgeCaseEvent.Save(e.cfg, e.db, topicConfig, artie.NewMessage(&newKafkaMsg, newKafkaMsg.Topic))
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData("foo")
	inMemCol, isOk := td.ReadOnlyInMemoryCols().GetColumn(badColumn)
	assert.True(e.T(), isOk)
	assert.Equal(e.T(), typing.Invalid, inMemCol.KindDetails)
}

func (e *EventsTestSuite) TestEvent_SaveCasing() {
	event := Event{
		Table:       "foo",
		primaryKeys: []string{"id"},
		Data: map[string]any{
			"id":                                "123",
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"randomCol":                         "dusty",
			"anotherCOL":                        13.37,
		},
	}

	kafkaMsg := kafka.Message{}
	_, _, err := event.Save(e.cfg, e.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData("foo")
	var rowData map[string]any
	for _, row := range td.Rows() {
		if id, ok := row.GetValue("id"); ok {
			if id == "123" {
				rowData = row.GetData()
			}
		}
	}

	for _, expectedColumn := range []string{"randomcol", "anothercol"} {
		_, isOk := rowData[expectedColumn]
		assert.True(e.T(), isOk, fmt.Sprintf("expected col: %s, rowsData: %v", expectedColumn, rowData))
	}

}

func (e *EventsTestSuite) TestEventSaveOptionalSchema() {
	event := Event{
		Table:       "foo",
		primaryKeys: []string{"id"},
		Data: map[string]any{
			"id":                                "123",
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"randomCol":                         "dusty",
			"anotherCOL":                        13.37,
			"created_at_date_string":            "2023-01-01",
			"created_at_date_no_schema":         "2023-01-01",
			"json_object_string":                `{"foo": "bar"}`,
			"json_object_no_schema":             `{"foo": "bar"}`,
		},
		OptionalSchema: map[string]typing.KindDetails{
			// Explicitly casting this as a string.
			"created_at_date_string": typing.String,
			"json_object_string":     typing.String,
		},
	}

	kafkaMsg := kafka.Message{}
	_, _, err := event.Save(e.cfg, e.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData("foo")
	{
		// Optional schema w/ string
		column, isOk := td.ReadOnlyInMemoryCols().GetColumn("created_at_date_string")
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.String, column.KindDetails)

		// String (with created_at datetime type)
		column, isOk = td.ReadOnlyInMemoryCols().GetColumn("created_at_date_no_schema")
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.String, column.KindDetails)
	}
	{
		// JSON string
		column, isOk := td.ReadOnlyInMemoryCols().GetColumn("json_object_string")
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.String, column.KindDetails)
	}
	{
		// JSON
		column, isOk := td.ReadOnlyInMemoryCols().GetColumn("json_object_no_schema")
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.Struct, column.KindDetails)
	}
}

func (e *EventsTestSuite) TestEvent_SaveColumnsNoData() {
	var cols columns.Columns
	for i := 0; i < 50; i++ {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col_%d", i), typing.Invalid))
	}

	evt := Event{
		Table:   "non_existent",
		Columns: &cols,
		Data: map[string]any{
			"col_1":                             "123",
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
		},
		primaryKeys: []string{"col_1"},
	}

	kafkaMsg := kafka.Message{}
	_, _, err := evt.Save(e.cfg, e.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData("non_existent")
	var prevKey string
	for _, col := range td.ReadOnlyInMemoryCols().GetColumns() {
		if col.Name() == constants.DeleteColumnMarker || col.Name() == constants.OnlySetDeleteColumnMarker {
			continue
		}

		columnNamePart := strings.Split(col.Name(), "_")[1]

		if prevKey == "" {
			prevKey = columnNamePart
			continue
		}

		currentKeyParsed, err := strconv.Atoi(columnNamePart)
		assert.NoError(e.T(), err)

		prevKeyParsed, err := strconv.Atoi(prevKey)
		assert.NoError(e.T(), err)

		// Testing ordering.
		assert.True(e.T(), currentKeyParsed > prevKeyParsed, fmt.Sprintf("current key: %q, prevKey: %q", currentKeyParsed, prevKeyParsed))
	}

	// Now let's add more keys.
	evt.Columns.AddColumn(columns.NewColumn("foo", typing.Invalid))
	var index int
	for idx, col := range evt.Columns.GetColumns() {
		if col.Name() == "foo" {
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
		Table:       "foo",
		Columns:     &cols,
		primaryKeys: []string{"id"},
		Data: map[string]any{
			"id":                                "123",
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			"randomCol":                         "dusty",
			"anotherCOL":                        13.37,
			"created_at_date_string":            "2023-01-01",
		},
	}

	kafkaMsg := kafka.Message{}
	_, _, err := event.Save(e.cfg, e.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData("foo")
	{
		// String
		column, isOk := td.ReadOnlyInMemoryCols().GetColumn("randomcol")
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.String, column.KindDetails)
	}
	{
		// Number
		column, isOk := td.ReadOnlyInMemoryCols().GetColumn("anothercol")
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.Float, column.KindDetails)
	}
	{
		// String
		column, isOk := td.ReadOnlyInMemoryCols().GetColumn("created_at_date_string")
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.String, column.KindDetails)
	}
	{
		// Boolean
		column, isOk := td.ReadOnlyInMemoryCols().GetColumn(constants.DeleteColumnMarker)
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.Boolean, column.KindDetails)
	}
	{
		// Boolean
		column, isOk := td.ReadOnlyInMemoryCols().GetColumn(constants.OnlySetDeleteColumnMarker)
		assert.True(e.T(), isOk)
		assert.Equal(e.T(), typing.Boolean, column.KindDetails)
	}
}

func (e *EventsTestSuite) TestEventSaveTestDeleteFlag() {
	event := Event{
		Table:       "foo",
		primaryKeys: []string{"id"},
		Data: map[string]any{
			"id":                                "123",
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
		},
		Deleted: true,
	}

	kafkaMsg := kafka.Message{}
	_, _, err := event.Save(e.cfg, e.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
	assert.NoError(e.T(), err)
	assert.False(e.T(), e.db.GetOrCreateTableData("foo").ContainOtherOperations())

	event.Deleted = false
	_, _, err = event.Save(e.cfg, e.db, topicConfig, artie.NewMessage(&kafkaMsg, kafkaMsg.Topic))
	assert.NoError(e.T(), err)
	assert.True(e.T(), e.db.GetOrCreateTableData("foo").ContainOtherOperations())
}
