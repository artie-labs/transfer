package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

func TestTopicConfigFormatter_ShouldSkip(t *testing.T) {
	{
		// Multiple operations
		tc := kafkalib.TopicConfig{SkippedOperations: "c, r, u"}
		formatter := NewTopicConfigFormatter(tc, nil)
		for _, op := range []string{"c", "r", "u"} {
			assert.True(t, formatter.ShouldSkip(op))
		}
		assert.False(t, formatter.ShouldSkip("d"))
	}
	{
		// Single operation - create
		tc := kafkalib.TopicConfig{SkippedOperations: "c"}
		formatter := NewTopicConfigFormatter(tc, nil)
		assert.True(t, formatter.ShouldSkip("c"))
		assert.False(t, formatter.ShouldSkip("d"))
	}
	{
		// Single operation - delete
		tc := kafkalib.TopicConfig{SkippedOperations: "d"}
		formatter := NewTopicConfigFormatter(tc, nil)
		assert.True(t, formatter.ShouldSkip("d"))
		assert.False(t, formatter.ShouldSkip("c"))
	}
	{
		// No operations to skip
		tc := kafkalib.TopicConfig{SkippedOperations: ""}
		formatter := NewTopicConfigFormatter(tc, nil)
		assert.False(t, formatter.ShouldSkip("c"))
		assert.False(t, formatter.ShouldSkip("d"))
	}
	{
		// Whitespace handling
		tc := kafkalib.TopicConfig{SkippedOperations: "  c  ,  d  "}
		formatter := NewTopicConfigFormatter(tc, nil)
		assert.True(t, formatter.ShouldSkip("c"))
		assert.True(t, formatter.ShouldSkip("d"))
	}
	{
		// Case insensitivity
		tc := kafkalib.TopicConfig{SkippedOperations: "C, D"}
		formatter := NewTopicConfigFormatter(tc, nil)
		assert.True(t, formatter.ShouldSkip("c"))
		assert.True(t, formatter.ShouldSkip("d"))
	}
}

func TestTopicConfigFormatter_ShouldSkip_Panic(t *testing.T) {
	// Verify that directly creating TopicConfigFormatter without NewTopicConfigFormatter panics
	formatter := TopicConfigFormatter{}
	assert.PanicsWithValue(t, "skipOperationsMap is nil, NewTopicConfigFormatter() was never called", func() {
		formatter.ShouldSkip("c")
	})
}
