package event

import (
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func (e *EventsTestSuite) TestBuildPrimaryKeys() {
	{
		// No primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{}, []string{}, nil)
		assert.Empty(e.T(), pks)
	}
	{
		// Primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{PrimaryKeysOverride: []string{"id"}}, []string{}, nil)
		assert.Equal(e.T(), []string{"id"}, pks)
	}
	{
		// Include primary keys
		pks := buildPrimaryKeys(kafkalib.TopicConfig{IncludePrimaryKeys: []string{"id"}}, []string{}, nil)
		assert.Equal(e.T(), []string{"id"}, pks)
	}
	{
		// Include primary keys and primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{PrimaryKeysOverride: []string{}, IncludePrimaryKeys: []string{"id2"}}, []string{"id", "id2"}, nil)
		assert.ElementsMatch(e.T(), []string{"id", "id2"}, pks)
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
		{
			// Includes static columns
			data := transformData(map[string]any{"foo": "bar", "abc": "def"}, kafkalib.TopicConfig{ColumnsToInclude: []string{"foo"}, StaticColumns: []kafkalib.StaticColumn{{Name: "dusty", Value: "mini aussie"}}})
			assert.Equal(e.T(), map[string]any{"foo": "bar", "dusty": "mini aussie"}, data)
		}
	}
}

func (e *EventsTestSuite) TestBuildRowKey() {
	{
		// Happy path
		data := map[string]any{
			"a_id": 1,
			"b_id": 2,
		}

		rowKey, rowMap, err := buildRowKey([]string{"a_id", "b_id"}, data)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), "a_id=1b_id=2", rowKey)
		assert.Equal(e.T(), map[string]any{"a_id": 1, "b_id": 2}, rowMap)
	}
	{
		// Data does not exist in the row
		data := map[string]any{
			"a_id": 1,
		}

		rowKey, rowMap, err := buildRowKey([]string{"a_id", "b_id"}, data)
		assert.ErrorContains(e.T(), err, `primary key "b_id" not found in data: map[a_id:1]`)
		assert.Empty(e.T(), rowKey)
		assert.Empty(e.T(), rowMap)
	}
}

func (e *EventsTestSuite) TestBuildDeleteRow() {
	// Happy path
	data := map[string]any{
		"a_id": 1,
		"b_id": 2,
	}

	deleteRow, err := buildDeleteRow([]string{"a_id", "b_id"}, data)
	assert.NoError(e.T(), err)
	assert.Equal(e.T(), map[string]any{constants.DeleteColumnMarker: true, constants.OnlySetDeleteColumnMarker: true, "a_id": 1, "b_id": 2}, deleteRow)
}
