package jitter

import (
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

	// Cap the attempts to be 30 so we don't have integer overflows.
	if attemptsMaxMs := baseMs * powerOfTwo(max(attempts, 30)); attemptsMaxMs > 0 {
		maxMs = min(maxMs, attemptsMaxMs)
	}

	return time.Duration(rand.Intn(maxMs)) * time.Millisecond
}
