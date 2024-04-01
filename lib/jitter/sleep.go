package jitter

import (
	"math"
	"math/rand"
	"time"
)

const DefaultMaxMs = 3500

func powerOfTwo(n int) int {
	return 1 << n
}

func Jitter(baseMs, maxMs, attempts int) time.Duration {
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	// sleep = random_between(0, min(cap, base * 2 ** attempt))
	// 2 ** x == 1 << x
	if maxMs <= 0 {
		return time.Duration(0)
	}

	// maxThreshold is used to make sure the multiplier does not cause any value overflow.
	// The 1 million comes from `ms` and `ns` conversion.
	maxThreshold := math.MaxInt / baseMs / 1_000_000
	if multiplier := powerOfTwo(min(attempts, 20)); multiplier <= maxThreshold {
		maxMs = min(maxMs, baseMs*multiplier)
	}

	return time.Duration(rand.Intn(maxMs)) * time.Millisecond
}
