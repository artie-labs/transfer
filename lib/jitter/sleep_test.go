package jitter

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPowerOfTwo(t *testing.T) {
	assert.Equal(t, 1, powerOfTwo(0))
	assert.Equal(t, 2, powerOfTwo(1))
	assert.Equal(t, 4, powerOfTwo(2))
	assert.Equal(t, 8, powerOfTwo(3))
	assert.Equal(t, 16, powerOfTwo(4))
	assert.Equal(t, 32, powerOfTwo(5))
	assert.Equal(t, 64, powerOfTwo(6))
	assert.Equal(t, 128, powerOfTwo(7))
	assert.Equal(t, 256, powerOfTwo(8))
	assert.Equal(t, 512, powerOfTwo(9))
	assert.Equal(t, 1024, powerOfTwo(10))
}

func TestJitter(t *testing.T) {
	// maxMs <= 0 returns time.Duration(0)
	assert.Equal(t, time.Duration(0), Jitter(10, 0, 0))
	assert.Equal(t, time.Duration(0), Jitter(10, 0, 100))
	assert.Equal(t, time.Duration(0), Jitter(10, -1, 0))
	assert.Equal(t, time.Duration(0), Jitter(10, -1, 100))

	{
		// A large maxMs and large attempts
		value := Jitter(10, math.MaxInt, 200)
		assert.LessOrEqual(t, value, time.Duration(math.MaxInt/1_000_000)*time.Millisecond)
	}
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
