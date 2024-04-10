package jitter

import (
	"math/rand"
	"time"
)

const DefaultMaxMs = 3500

// computeJitterUpperBoundMs calculates min(maxMs, baseMs * 2 ** attempt)
func computeJitterUpperBoundMs(baseMs int64, maxMs int64, attempts int64) int64 {
	if maxMs <= 0 {
		return 0
	}

	// Check for overflows when computing base * 2 ** attempts.
	// 2 ** x == 1 << x
	if attemptsMaxMs := baseMs * (1 << attempts); attemptsMaxMs > 0 {
		maxMs = min(maxMs, attemptsMaxMs)
	}

	return maxMs
}

// Jitter implements exponential backoff + jitter.
// See: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
// sleep = random_between(0, min(cap, base * 2 ** attempt))
func Jitter(baseMs, maxMs, attempts int) time.Duration {
	upperBoundMs := computeJitterUpperBoundMs(int64(baseMs), int64(maxMs), int64(attempts))
	if upperBoundMs <= 0 {
		return time.Duration(0)
	}
	return time.Duration(rand.Int63n(upperBoundMs)) * time.Millisecond
}
