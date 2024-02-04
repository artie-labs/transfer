package jitter

import (
	"math/rand"
)

const maxMilliSeconds = 3500

func JitterMs(baseMilliSeconds, attempts int) int {
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	// sleep = random_between(0, min(cap, base * 2 ** attempt))
	// 2 ** x == 1 << x
	return rand.Intn(min(maxMilliSeconds, baseMilliSeconds*(1<<attempts)))
}
