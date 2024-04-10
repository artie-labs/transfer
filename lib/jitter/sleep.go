package jitter

import (
	"math/rand"
	"time"
)

const DefaultMaxMs = 3500

func computeJitterUpperBoundMs(baseMs, maxMs, attempts int) int {
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	// sleep = random_between(0, min(cap, base * 2 ** attempt))
	// 2 ** x == 1 << x
	if maxMs <= 0 {
		return 0
	}

	// Check for overflows when computing base * 2 ** attempts.
	if attemptsMaxMs := baseMs * (1 << attempts); attemptsMaxMs > 0 {
		maxMs = min(maxMs, attemptsMaxMs)
	}

	return maxMs
}

func Jitter(baseMs, maxMs, attempts int) time.Duration {
	upperBoundMs := computeJitterUpperBoundMs(baseMs, maxMs, attempts)
	if upperBoundMs <= 0 {
		return time.Duration(0)
	}
	return time.Duration(rand.Intn(upperBoundMs)) * time.Millisecond
}
