package event

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

var idMap = map[string]any{
	"id": 123,
}

func (e *EventsTestSuite) TestEvent_Validate() {
	{
		_evt := Event{table: "foo"}
		assert.ErrorContains(e.T(), _evt.Validate(), "primary keys are empty")
	}
	{
		_evt := Event{table: "foo", primaryKeys: []string{"id"}}
		assert.ErrorContains(e.T(), _evt.Validate(), "event has no data")
	}
	{
		_evt := Event{
			table:       "foo",
			primaryKeys: []string{"id"},
			data: map[string]any{
				"id":  123,
				"foo": "bar",
			},
			mode: config.History,
		}
		assert.NoError(e.T(), _evt.Validate())
	}
	{
		_evt := Event{
			table:       "foo",
			primaryKeys: []string{"id"},
			data: map[string]any{
				"id":  123,
				"foo": "bar",
			},
		}
		assert.ErrorContains(e.T(), _evt.Validate(), "")
	}
	{
		_evt := Event{
			table:       "foo",
			primaryKeys: []string{"id"},
			data: map[string]any{
				"id":                                123,
				constants.DeleteColumnMarker:        true,
				constants.OnlySetDeleteColumnMarker: true,
			},
		}
		assert.NoError(e.T(), _evt.Validate())
	}
}

func (e *EventsTestSuite) TestTransformData() {
	{
		// Hashing columns
		{
			// No columns to hash
			data := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{})
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// There's a column to hash, but the event does not have any data
			data := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"super duper"}})
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Hash the column foo (value is set)
			data := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"foo"}})
			assert.Equal(e.T(), map[string]any{"foo": "fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9", "abc": "def"}, data)
		}
		{
			// Hash the column foo (value is nil)
			data := transformData(map[string]any{"foo": nil, "abc": "def"}, kafkalib.TopicConfig{ColumnsToHash: []string{"foo"}})
			assert.Equal(e.T(), map[string]any{"foo": nil, "abc": "def"}, data)
		}
	}
	{
		// Excluding columns
		{
			// No columns to exclude
			data := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToExclude: []string{}})
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Exclude the column foo
			data := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToExclude: []string{"foo"}})
			assert.Equal(e.T(), map[string]any{"abc": "def"}, data)
		}
	}
	{
		// Include columns
		{
			// No columns to include
			data := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{}})
			assert.Equal(e.T(), map[string]any{"foo": "bar", "abc": "def"}, data)
		}
		{
			// Include the column foo
			data := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}})
			assert.Equal(e.T(), map[string]any{"foo": "bar"}, data)
		}
		{
			// include foo, but also artie columns
			data := transformData(map[string]any{"foo": "bar", "abc": "def", constants.DeleteColumnMarker: true}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}})
			assert.Equal(e.T(), map[string]any{"foo": "bar", constants.DeleteColumnMarker: true}, data)
		}
	}
}

func testBuildFilteredColumns(t *testing.T, fakeEvent *mocks.FakeEvent, topicConfig kafkalib.TopicConfig, fakeColumns []columns.Column, expectedCols *columns.Columns) {
	fakeEvent.GetColumnsReturns(columns.NewColumns(fakeColumns), nil)

	cols, err := buildFilteredColumns(fakeEvent, topicConfig)
	assert.NoError(t, err)
	assert.Equal(t, expectedCols.GetColumns(), cols.GetColumns())
}

func (e *EventsTestSuite) TestBuildFilteredColumns() {
	{
		// Not excluding or including anything
		fakeCols := []columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
		}
		testBuildFilteredColumns(e.T(), e.fakeEvent, kafkalib.TopicConfig{}, fakeCols, columns.NewColumns(fakeCols))
	}
	{
		// Exclude foo
		fakeCols := []columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
		}
		testBuildFilteredColumns(e.T(), e.fakeEvent, kafkalib.TopicConfig{ColumnsToExclude: []string{"foo"}}, fakeCols, columns.NewColumns([]columns.Column{
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
		}))
	}
	{
		// Include foo
		fakeCols := []columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
		}
		testBuildFilteredColumns(e.T(), e.fakeEvent, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}}, fakeCols, columns.NewColumns([]columns.Column{
			columns.NewColumn("foo", typing.String),
		}))
	}
	{
		// Include foo, but also artie columns
		fakeCols := []columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn("bar", typing.String),
			columns.NewColumn("baz", typing.String),
			columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		}
		testBuildFilteredColumns(e.T(), e.fakeEvent, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}}, fakeCols, columns.NewColumns([]columns.Column{
			columns.NewColumn("foo", typing.String),
			columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		}))
	}
}

func (e *EventsTestSuite) TestEvent_TableName() {
	{
		// Don't pass in tableName.
		evt, err := ToMemoryEvent(e.fakeEvent, idMap, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), e.fakeEvent.GetTableName(), evt.GetTable())
	}
	{
		// Now pass it in, it should override.
		evt, err := ToMemoryEvent(e.fakeEvent, idMap, kafkalib.TopicConfig{TableName: "orders"}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "orders", evt.GetTable())
	}
	{
		// Now, if it's history mode...
		evt, err := ToMemoryEvent(e.fakeEvent, idMap, kafkalib.TopicConfig{TableName: "orders"}, config.History)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "orders__history", evt.GetTable())

		// Table already has history suffix, so it won't add extra.
		evt, err = ToMemoryEvent(e.fakeEvent, idMap, kafkalib.TopicConfig{TableName: "dusty__history"}, config.History)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "dusty__history", evt.GetTable())
	}
}

func (e *EventsTestSuite) TestEvent_Columns() {
	{
		evt, err := ToMemoryEvent(e.fakeEvent, map[string]any{"id": 123}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)

		assert.Equal(e.T(), 1, len(evt.columns.GetColumns()))
		_, ok := evt.columns.GetColumn("id")
		assert.True(e.T(), ok)
	}
	{
		// Now it should handle escaping column names
		evt, err := ToMemoryEvent(e.fakeEvent, map[string]any{"id": 123, "CAPITAL": "foo"}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)

		assert.Equal(e.T(), 2, len(evt.columns.GetColumns()))
		_, ok := evt.columns.GetColumn("id")
		assert.True(e.T(), ok)

		_, ok = evt.columns.GetColumn("capital")
		assert.True(e.T(), ok)
	}
	{
		// In history mode, the deletion column markers should be removed from the event data
		evt, err := ToMemoryEvent(e.fakeEvent, map[string]any{"id": 123}, kafkalib.TopicConfig{}, config.History)
		assert.NoError(e.T(), err)

		_, ok := evt.data[constants.DeleteColumnMarker]
		assert.False(e.T(), ok)
		_, ok = evt.data[constants.OnlySetDeleteColumnMarker]
		assert.False(e.T(), ok)
	}
}

func (e *EventsTestSuite) TestEventPrimaryKeys() {
	evt := &Event{
		table:       "foo",
		primaryKeys: []string{"id", "id1", "id2", "id3", "id4"},
	}

	requiredKeys := []string{"id", "id1", "id2", "id3", "id4"}
	for _, requiredKey := range requiredKeys {
		var found bool
		for _, primaryKey := range evt.GetPrimaryKeys() {
			found = requiredKey == primaryKey
			if found {
				break
			}
		}

		assert.True(e.T(), found, requiredKey)
	}

	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns("foo")
	mockEvent.GetDataReturns(map[string]any{"id": 1, "course_id": 2}, nil)

	anotherEvt, err := ToMemoryEvent(mockEvent, map[string]any{"id": 1, "course_id": 2}, kafkalib.TopicConfig{}, config.Replication)
	assert.NoError(e.T(), err)

	pkValue, err := anotherEvt.PrimaryKeyValue()
	assert.NoError(e.T(), err)
	assert.Equal(e.T(), "course_id=2id=1", pkValue)

	// Make sure the ordering for the pk is deterministic.
	partsMap := make(map[string]bool)
	for i := 0; i < 100; i++ {
		pkValue, err := anotherEvt.PrimaryKeyValue()
		assert.NoError(e.T(), err)
		partsMap[pkValue] = true
	}

	assert.Equal(e.T(), len(partsMap), 1)

	// If the value doesn't exist in the event payload
	{
		mockEvent := &mocks.FakeEvent{}
		mockEvent.GetTableNameReturns("foo")
		mockEvent.GetDataReturns(map[string]any{"course_id": 2}, nil)

		evt, err := ToMemoryEvent(mockEvent, map[string]any{"id": 123}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)

		pkValue, err := evt.PrimaryKeyValue()
		assert.ErrorContains(e.T(), err, `primary key "id" not found in data: map[course_id:2]`)
		assert.Equal(e.T(), "", pkValue)
	}
}

func (e *EventsTestSuite) TestPrimaryKeyValueDeterministic() {
	mockEvent := &mocks.FakeEvent{}
	mockEvent.GetTableNameReturns("foo")
	mockEvent.GetDataReturns(map[string]any{
		"aa":    1,
		"bb":    5,
		"zz":    "ff",
		"gg":    "artie",
		"dusty": "mini aussie",
	}, nil)

	evt, err := ToMemoryEvent(mockEvent, map[string]any{
		"aa":    1,
		"bb":    5,
		"zz":    "ff",
		"gg":    "artie",
		"dusty": "mini aussie",
	}, kafkalib.TopicConfig{}, config.Replication)
	assert.NoError(e.T(), err)

	for i := 0; i < 50_000; i++ {
		pkValue, err := evt.PrimaryKeyValue()
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "aa=1bb=5dusty=mini aussiegg=artiezz=ff", pkValue)
	}
}

func (e *EventsTestSuite) TestEvent_PrimaryKeysOverride() {
	{
		// No primary keys override
		evt, err := ToMemoryEvent(e.fakeEvent, map[string]any{"not_id": 123}, kafkalib.TopicConfig{}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), []string{"not_id"}, evt.GetPrimaryKeys())
	}
	{
		// Specified primary keys override
		evt, err := ToMemoryEvent(e.fakeEvent, map[string]any{"not_id": 123}, kafkalib.TopicConfig{PrimaryKeysOverride: []string{"id"}}, config.Replication)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), []string{"id"}, evt.GetPrimaryKeys())
	}
}
