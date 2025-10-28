package event

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
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

	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns(topicConfig.TableName)
	mockEvent.GetDataReturns(map[string]any{
		"id":                                "123",
		constants.DeleteColumnMarker:        true,
		constants.OnlySetDeleteColumnMarker: true,
		expectedCol:                         "dusty",
		anotherCol:                          13.37,
	}, nil)

	event, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, mockEvent, map[string]any{"id": "123"}, topicConfig, config.Replication)
	assert.NoError(e.T(), err)

	_, _, err = event.Save(e.cfg, e.db, topicConfig, nil)
	assert.NoError(e.T(), err)

	optimization := e.db.GetOrCreateTableData(event.GetTableID(), topicConfig.Topic)
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
		table:       "foo",
		primaryKeys: []string{"id"},
		data: map[string]any{
			"id":                                "12344",
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: true,
			expectedCol:                         "dusty",
			anotherCol:                          13.37,
			badColumn:                           constants.ToastUnavailableValuePlaceholder,
		},
	}

	_, _, err = edgeCaseEvent.Save(e.cfg, e.db, topicConfig, nil)
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData(edgeCaseEvent.GetTableID(), topicConfig.Topic)
	inMemCol, ok := td.ReadOnlyInMemoryCols().GetColumn(badColumn)
	assert.True(e.T(), ok)
	assert.Equal(e.T(), typing.Invalid, inMemCol.KindDetails)
}

func (e *EventsTestSuite) TestEvent_SaveCasing() {
	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns(topicConfig.TableName)
	mockEvent.GetDataReturns(map[string]any{
		"id":                                "123",
		constants.DeleteColumnMarker:        true,
		constants.OnlySetDeleteColumnMarker: true,
		"randomCol":                         "dusty",
		"anotherCOL":                        13.37,
	}, nil)

	event, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, mockEvent, map[string]any{"id": "123"}, topicConfig, config.Replication)
	assert.NoError(e.T(), err)

	_, _, err = event.Save(e.cfg, e.db, topicConfig, nil)
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData(event.GetTableID(), topicConfig.Topic)
	var rowData map[string]any
	for _, row := range td.Rows() {
		if id, ok := row.GetValue("id"); ok {
			if id == "123" {
				rowData = row.GetData()
			}
		}
	}

	for _, expectedColumn := range []string{"randomcol", "anothercol"} {
		_, ok := rowData[expectedColumn]
		assert.True(e.T(), ok, fmt.Sprintf("expected col: %s, rowsData: %v", expectedColumn, rowData))
	}
}

func (e *EventsTestSuite) TestEventSaveOptionalSchema() {
	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns(topicConfig.TableName)
	mockEvent.GetDataReturns(map[string]any{
		"id":                                "123",
		constants.DeleteColumnMarker:        true,
		constants.OnlySetDeleteColumnMarker: true,
		"randomCol":                         "dusty",
		"anotherCOL":                        13.37,
		"created_at_date_string":            "2023-01-01",
		"created_at_date_no_schema":         "2023-01-01",
		"json_object_string":                `{"foo": "bar"}`,
		"json_object_no_schema":             `{"foo": "bar"}`,
	}, nil)
	mockEvent.GetOptionalSchemaReturns(map[string]typing.KindDetails{
		"created_at_date_string": typing.String,
		"json_object_string":     typing.String,
	}, nil)

	event, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, mockEvent, map[string]any{"id": "123"}, topicConfig, config.Replication)
	assert.NoError(e.T(), err)

	_, _, err = event.Save(e.cfg, e.db, topicConfig, nil)
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData(event.GetTableID(), topicConfig.Topic)
	{
		// Optional schema w/ string
		column, ok := td.ReadOnlyInMemoryCols().GetColumn("created_at_date_string")
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.String, column.KindDetails)

		// String (with created_at datetime type)
		column, ok = td.ReadOnlyInMemoryCols().GetColumn("created_at_date_no_schema")
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.String, column.KindDetails)
	}
	{
		// JSON string
		column, ok := td.ReadOnlyInMemoryCols().GetColumn("json_object_string")
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.String, column.KindDetails)
	}
	{
		// JSON
		column, ok := td.ReadOnlyInMemoryCols().GetColumn("json_object_no_schema")
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.Struct, column.KindDetails)
	}
}

func (e *EventsTestSuite) TestEvent_SaveColumnsNoData() {
	var cols columns.Columns
	for i := range 50 {
		cols.AddColumn(columns.NewColumn(fmt.Sprintf("col_%d", i), typing.Invalid))
	}

	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns(topicConfig.TableName)
	mockEvent.GetDataReturns(map[string]any{
		"col_1":                             "123",
		constants.DeleteColumnMarker:        true,
		constants.OnlySetDeleteColumnMarker: true,
	}, nil)
	mockEvent.GetColumnsReturns(&cols, nil)

	evt, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, mockEvent, map[string]any{"col_1": "123"}, topicConfig, config.Replication)
	assert.NoError(e.T(), err)

	_, _, err = evt.Save(e.cfg, e.db, topicConfig, nil)
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData(evt.GetTableID(), topicConfig.Topic)
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
	evt.columns.AddColumn(columns.NewColumn("foo", typing.Invalid))
	var index int
	for idx, col := range evt.columns.GetColumns() {
		if col.Name() == "foo" {
			index = idx
		}
	}

	assert.Equal(e.T(), len(evt.columns.GetColumns())-1, index, "new column inserted to the end")
}

func (e *EventsTestSuite) TestEventSaveColumns() {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("randomCol", typing.Invalid))
	cols.AddColumn(columns.NewColumn("anotherCOL", typing.Invalid))
	cols.AddColumn(columns.NewColumn("created_at_date_string", typing.Invalid))

	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns(topicConfig.TableName)
	mockEvent.GetColumnsReturns(&cols, nil)
	mockEvent.GetDataReturns(map[string]any{
		"id":                                "123",
		constants.DeleteColumnMarker:        true,
		constants.OnlySetDeleteColumnMarker: true,
		"randomCol":                         "dusty",
		"anotherCOL":                        13.37,
		"created_at_date_string":            "2023-01-01",
	}, nil)

	event, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, mockEvent, map[string]any{"id": "123"}, topicConfig, config.Replication)
	assert.NoError(e.T(), err)

	_, _, err = event.Save(e.cfg, e.db, topicConfig, nil)
	assert.NoError(e.T(), err)

	td := e.db.GetOrCreateTableData(event.GetTableID(), topicConfig.Topic)
	{
		// String
		column, ok := td.ReadOnlyInMemoryCols().GetColumn("randomcol")
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.String, column.KindDetails)
	}
	{
		// Number
		column, ok := td.ReadOnlyInMemoryCols().GetColumn("anothercol")
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.Float, column.KindDetails)
	}
	{
		// String
		column, ok := td.ReadOnlyInMemoryCols().GetColumn("created_at_date_string")
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.String, column.KindDetails)
	}
	{
		// Boolean
		column, ok := td.ReadOnlyInMemoryCols().GetColumn(constants.DeleteColumnMarker)
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.Boolean, column.KindDetails)
	}
	{
		// Boolean
		column, ok := td.ReadOnlyInMemoryCols().GetColumn(constants.OnlySetDeleteColumnMarker)
		assert.True(e.T(), ok)
		assert.Equal(e.T(), typing.Boolean, column.KindDetails)
	}
}

func (e *EventsTestSuite) TestEventSaveTestDeleteFlag() {
	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns(topicConfig.TableName)
	mockEvent.DeletePayloadReturns(true)
	mockEvent.GetDataReturns(map[string]any{
		"id":                                "123",
		constants.DeleteColumnMarker:        true,
		constants.OnlySetDeleteColumnMarker: true,
	}, nil)

	event, err := ToMemoryEvent(e.T().Context(), e.fakeBaseline, mockEvent, map[string]any{"id": "123"}, topicConfig, config.Replication)
	assert.NoError(e.T(), err)
	_, _, err = event.Save(e.cfg, e.db, topicConfig, nil)
	assert.NoError(e.T(), err)
	assert.False(e.T(), e.db.GetOrCreateTableData(event.GetTableID(), topicConfig.Topic).ContainOtherOperations())
	assert.True(e.T(), e.db.GetOrCreateTableData(event.GetTableID(), topicConfig.Topic).ContainsHardDeletes())

	event.deleted = false
	_, _, err = event.Save(e.cfg, e.db, topicConfig, nil)
	assert.NoError(e.T(), err)
	assert.True(e.T(), e.db.GetOrCreateTableData(event.GetTableID(), topicConfig.Topic).ContainOtherOperations())
}
