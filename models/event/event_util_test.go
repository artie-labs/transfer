package event

import (
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
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
