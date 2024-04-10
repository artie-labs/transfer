package jitter

import (
	"math"
	"math/rand"
	"time"
)

const DefaultMaxMs = 3500

// safePowerOfTwo calculates 2 ** n without panicking for values of n below 0 or above 62.
func safePowerOfTwo(n int64) int64 {
	if n < 0 {
		return 0
	} else if n > 62 {
		return math.MaxInt64 // 2 ** n will overflow
	}
	return 1 << n // 2 ** n == 1 << n
}

// computeJitterUpperBoundMs calculates min(maxMs, baseMs * 2 ** attempt).
func computeJitterUpperBoundMs(baseMs, maxMs, attempts int64) int64 {
	if maxMs <= 0 {
		return 0
	}

	powerOfTwo := safePowerOfTwo(attempts)
	if powerOfTwo > math.MaxInt64/baseMs { // check for overflow
		return maxMs
	}
	return min(maxMs, baseMs*powerOfTwo)
}

// Jitter implements exponential backoff + jitter.
// See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
// Algorithm: sleep = random_between(0, min(cap, base * 2 ** attempt))
func Jitter(baseMs, maxMs, attempts int) time.Duration {
	upperBoundMs := computeJitterUpperBoundMs(int64(baseMs), int64(maxMs), int64(attempts))
	if upperBoundMs <= 0 {
		return time.Duration(0)
	}
	return time.Duration(rand.Int63n(upperBoundMs)) * time.Millisecond
}
