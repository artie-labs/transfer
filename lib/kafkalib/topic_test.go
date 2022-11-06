package kafkalib

import (
	"github.com/stretchr/testify/assert"
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
