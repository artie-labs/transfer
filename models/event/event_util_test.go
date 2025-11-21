package event

import (
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func (e *EventsTestSuite) TestBuildPrimaryKeys() {
	{
		// No primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{}, map[string]any{}, nil)
		assert.Empty(e.T(), pks)
	}
	{
		// Primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{PrimaryKeysOverride: []string{"id"}}, map[string]any{}, nil)
		assert.Equal(e.T(), []string{"id"}, pks)
	}
	{
		// Include primary keys
		pks := buildPrimaryKeys(kafkalib.TopicConfig{IncludePrimaryKeys: []string{"id"}}, map[string]any{}, nil)
		assert.Equal(e.T(), []string{"id"}, pks)
	}
	{
		// Include primary keys and primary keys override
		pks := buildPrimaryKeys(kafkalib.TopicConfig{PrimaryKeysOverride: []string{}, IncludePrimaryKeys: []string{"id2"}}, map[string]any{"id": "123", "id2": "456"}, nil)
		assert.Equal(e.T(), []string{"id", "id2"}, pks)
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
