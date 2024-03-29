package jitter

import (
	"math/rand"
	"time"
)

const DefaultMaxMs = 3500

func Jitter(baseMs, maxMs, attempts int) time.Duration {
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	// sleep = random_between(0, min(cap, base * 2 ** attempt))
	// 2 ** x == 1 << x
	if maxMs <= 0 {
		return time.Duration(0)
	}

	// Check for overflows when computing base * 2 ** attempts.
	if attemptsMaxMs := baseMs * (1 << attempts); attemptsMaxMs > 0 {
		maxMs = min(maxMs, attemptsMaxMs)
	}

	ms := rand.Intn(maxMs)
	return time.Duration(ms) * time.Millisecond
}
