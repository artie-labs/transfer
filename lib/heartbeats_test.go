package lib

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHeartbeats(t *testing.T) {
	{
		// Nothing kicked off because the initial delay is not passed
		heartbeats := NewHeartbeats(1*time.Second, 1*time.Second, "test", map[string]any{"test": "test"})
		heartbeats.test = true
		cancel := heartbeats.Start()

		time.Sleep(300 * time.Millisecond)
		cancel()
		assert.Equal(t, 0, heartbeats.ticks)
	}
	{
		// Nothing kicked off because the initial delay is not passed
		heartbeats := NewHeartbeats(1*time.Second, 1*time.Second, "test", map[string]any{"test": "test"})
		heartbeats.Start()
	}
}
