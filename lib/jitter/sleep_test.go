package jitter

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestComputeJitterUpperBoundMs(t *testing.T) {
	// A maxMs that is <= 0 returns 0.
	assert.Equal(t, 0, computeJitterUpperBoundMs(0, 0, 0))
	assert.Equal(t, 0, computeJitterUpperBoundMs(10, 0, 0))
	assert.Equal(t, 0, computeJitterUpperBoundMs(10, 0, 100))
	assert.Equal(t, 0, computeJitterUpperBoundMs(10, -1, 0))
	assert.Equal(t, 0, computeJitterUpperBoundMs(10, -1, 100))

	// Increasing attempts with a baseMs of 10 and essentially no maxMs.
	assert.Equal(t, 10, computeJitterUpperBoundMs(10, math.MaxInt, 0))
	assert.Equal(t, 20, computeJitterUpperBoundMs(10, math.MaxInt, 1))
	assert.Equal(t, 40, computeJitterUpperBoundMs(10, math.MaxInt, 2))
	assert.Equal(t, 80, computeJitterUpperBoundMs(10, math.MaxInt, 3))
	assert.Equal(t, 160, computeJitterUpperBoundMs(10, math.MaxInt, 4))

	// Large inputs do not panic.
	assert.Equal(t, 100, computeJitterUpperBoundMs(10, 100, 200))
	assert.Equal(t, 100, computeJitterUpperBoundMs(10, 100, math.MaxInt))
	assert.Equal(t, math.MaxInt, computeJitterUpperBoundMs(math.MaxInt, math.MaxInt, math.MaxInt))
}

func TestJitter(t *testing.T) {
	// An upper bounds of 0 does not cause a [rand.Intn] panic.
	assert.Equal(t, time.Duration(0), Jitter(0, 0, 0))
	assert.Equal(t, time.Duration(0), Jitter(-1, -1, -1))

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
