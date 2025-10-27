package models

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/optimization"
)

func TestShouldSkipMerge(t *testing.T) {
	// 5 seconds
	coolDown := 5 * time.Second
	checkInterval := 200 * time.Millisecond

	td := TableData{
		TableData: &optimization.TableData{},
	}

	// Before wiping, we should not skip the flush since ts did not get set yet.
	assert.False(t, td.ShouldSkipFlush(coolDown))
	td.Wipe()
	for i := 0; i < 10; i++ {
		assert.True(t, td.ShouldSkipFlush(coolDown))
		time.Sleep(checkInterval)
	}

	time.Sleep(3 * time.Second)
	assert.False(t, td.ShouldSkipFlush(coolDown))

	// 5 minutes now
	coolDown = 5 * time.Minute
	now := time.Now()

	// We flushed 4 min ago, so let's test the confidence interval.
	td.lastFlushTime = now.Add(-4 * time.Minute)
	assert.False(t, td.ShouldSkipFlush(coolDown))

	// Let's try if we flushed 2 min ago, we should skip.
	td.lastFlushTime = now.Add(-2 * time.Minute)
	assert.True(t, td.ShouldSkipFlush(coolDown))
}
