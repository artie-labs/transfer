package models

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/stretchr/testify/assert"
)

func TestShouldSkipMerge(t *testing.T) {
	// 5 seconds
	coolDown := 5 * time.Second
	checkInterval := 200 * time.Millisecond

	td := TableData{
		TableData: &optimization.TableData{},
	}

	// Before wiping, we should not skip the merge since ts did not get set yet.
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

	// We merged 4 mins ago, so let's test the confidence interval.
	td.lastFlushTime = now.Add(-4 * time.Minute)
	assert.False(t, td.ShouldSkipFlush(coolDown))

	// Let's try if we merged 2 mins ago, we should skip.
	td.lastFlushTime = now.Add(-2 * time.Minute)
	assert.True(t, td.ShouldSkipFlush(coolDown))
}
