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
