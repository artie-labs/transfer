package lib

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const oneHundredMillisecond = 100 * time.Millisecond

func TestHeartbeats(t *testing.T) {
	{
		// Nothing kicked off because the initial delay is not passed
		heartbeats := NewHeartbeats(5*oneHundredMillisecond, oneHundredMillisecond, "test", map[string]any{"test": "test"})
		heartbeats.test = true
		done := heartbeats.Start()

		time.Sleep(3 * oneHundredMillisecond)
		done()
		assert.Zero(t, heartbeats.ticks)
	}
	{
		heartbeats := NewHeartbeats(oneHundredMillisecond, 2*oneHundredMillisecond, "test", map[string]any{"test": "test"})
		heartbeats.test = true
		done := heartbeats.Start()

		// Sleep 500 ms + buffer of 25 ms to make sure we hit the second tick
		time.Sleep(5*oneHundredMillisecond + 25*time.Millisecond)
		done()
		assert.Equal(t, 2, heartbeats.ticks)
	}
}
