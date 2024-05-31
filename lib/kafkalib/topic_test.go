package kafkalib

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetUniqueTopicConfigs(t *testing.T) {
	{
		// No topic configs
		assert.Empty(t, GetUniqueTopicConfigs(nil))
	}
	{
		// 1 topic config
		tcs := []*TopicConfig{
			{
				Database: "db",
				Schema:   "schema",
			},
		}

		actual := GetUniqueTopicConfigs(tcs)
		assert.Len(t, actual, 1)
		assert.Equal(t, *tcs[0], actual[0])
	}
	{
		// 2 topic configs (both the same)
		tcs := []*TopicConfig{
			{
				Database: "db",
				Schema:   "schema",
			},
			{
				Database: "db",
				Schema:   "schema",
			},
		}

		actual := GetUniqueTopicConfigs(tcs)
		assert.Len(t, actual, 1)
		assert.Equal(t, *tcs[0], actual[0])
	}
	{
		// 3 topic configs (2 the same)
		tcs := []*TopicConfig{
			{
				Database: "db",
				Schema:   "schema",
			},
			{
				Database: "db",
				Schema:   "schema",
			},
			{
				Database: "db",
				Schema:   "schema2",
			},
		}

		actual := GetUniqueTopicConfigs(tcs)
		assert.Len(t, actual, 2)
		assert.Equal(t, *tcs[0], actual[0])
		assert.Equal(t, *tcs[2], actual[1])
	}
}

func TestTopicConfig_String(t *testing.T) {
	tc := TopicConfig{
		Database:          "aaa",
		TableName:         "bbb",
		Schema:            "ccc",
		Topic:             "d",
		IdempotentKey:     "e",
		CDCFormat:         "f",
		SkippedOperations: "d",
	}

	assert.Contains(t, tc.String(), fmt.Sprintf("tableNameOverride=%s", tc.TableName), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("db=%s", tc.Database), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("schema=%s", tc.Schema), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("topic=%s", tc.Topic), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("idempotentKey=%s", tc.IdempotentKey), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("cdcFormat=%s", tc.CDCFormat), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("skippedOperations=%s", tc.SkippedOperations), tc.String())
}

func TestTopicConfig_Validate(t *testing.T) {
	var tc TopicConfig
	assert.ErrorContains(t, tc.Validate(), "database, schema, topic or cdc format is empty", tc.String())

	tc = TopicConfig{
		Database:     "12",
		TableName:    "34",
		Schema:       "56",
		Topic:        "78",
		CDCFormat:    "aa",
		CDCKeyFormat: JSONKeyFmt,
	}

	assert.ErrorContains(t, tc.Validate(), "opsToSkipMap is nil, call Load() first")

	tc.Load()
	assert.NoError(t, tc.Validate(), tc.String())

	tc.CDCKeyFormat = "non_existent"
	assert.ErrorContains(t, tc.Validate(), "invalid cdc key format: non_existent", tc.String())

	for _, validKeyFormat := range validKeyFormats {
		tc.CDCKeyFormat = validKeyFormat
		assert.NoError(t, tc.Validate(), tc.String())
	}
}

func TestTopicConfig_Load_ShouldSkip(t *testing.T) {
	{
		tc := TopicConfig{
			SkippedOperations: "c, r, u",
		}
		tc.Load()
		for _, op := range []string{"c", "r", "u"} {
			assert.True(t, tc.ShouldSkip(op), tc.String())
		}
		assert.False(t, tc.ShouldSkip("d"), tc.String())
	}
	{
		tc := TopicConfig{
			SkippedOperations: "c",
		}
		tc.Load()
		assert.True(t, tc.ShouldSkip("c"), tc.String())
	}
	{
		tc := TopicConfig{
			SkippedOperations: "d",
		}
		tc.Load()
		assert.True(t, tc.ShouldSkip("d"), tc.String())
	}
}
