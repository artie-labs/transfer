package jitter

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestJitter(t *testing.T) {
	// maxMs <= 0 returns time.Duration(0)
	assert.Equal(t, time.Duration(0), Jitter(10, 0, 0))
	assert.Equal(t, time.Duration(0), Jitter(10, 0, 100))
	assert.Equal(t, time.Duration(0), Jitter(10, -1, 0))
	assert.Equal(t, time.Duration(0), Jitter(10, -1, 100))

	{
		// A large number of attempts does not panic.
		value := Jitter(10, 100, 200)
		assert.LessOrEqual(t, value, time.Duration(100)*time.Millisecond)
	}
	{
		// A very large number of attempts does not panic.
		value := Jitter(10, 100, math.MaxInt)
		assert.LessOrEqual(t, value, time.Duration(100)*time.Millisecond)
	}
}
