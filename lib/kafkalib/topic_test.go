package kafkalib

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestToCacheKey(t *testing.T) {
	topicCfg := &TopicConfig{
		Database:  "db",
		TableName: "order",
		Schema:    "public",
		Topic:     "order",
	}

	assert.Equal(t, topicCfg.ToCacheKey(9), ToCacheKey("order", 9))
}

func TestTopicConfig_String(t *testing.T) {
	tc := TopicConfig{
		Database:      "aaa",
		TableName:     "bbb",
		Schema:        "ccc",
		Topic:         "d",
		IdempotentKey: "e",
		CDCFormat:     "f",
	}

	assert.True(t, strings.Contains(tc.String(), tc.TableName), tc.String())
	assert.True(t, strings.Contains(tc.String(), tc.Database), tc.String())
	assert.True(t, strings.Contains(tc.String(), tc.Schema), tc.String())
	assert.True(t, strings.Contains(tc.String(), tc.Topic), tc.String())
	assert.True(t, strings.Contains(tc.String(), tc.IdempotentKey), tc.String())
	assert.True(t, strings.Contains(tc.String(), tc.CDCFormat), tc.String())
}

func TestTopicConfig_Validate(t *testing.T) {
	var tc TopicConfig
	assert.False(t, tc.Valid(), tc.String())

	tc = TopicConfig{
		Database:  "12",
		TableName: "34",
		Schema:    "56",
		Topic:     "78",
		CDCFormat: "aa",
	}

	assert.True(t, tc.Valid(), tc.String())

	tc.CDCKeyFormat = "non_existent"
	assert.False(t, tc.Valid(), tc.String())

	for _, validKeyFormat := range validKeyFormats {
		tc.CDCKeyFormat = validKeyFormat
		assert.True(t, tc.Valid(), tc.String())
	}
}
