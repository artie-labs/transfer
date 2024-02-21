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
	ms := rand.Intn(min(maxMs, baseMs*(1<<attempts)))
	return time.Duration(ms) * time.Millisecond
}
