package jitter

import (
	"golang.org/x/exp/rand"
	"math"
)

const maxMilliSeconds = 3500

func JitterMs(baseMilliSeconds, attempts int) int {
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	// sleep = random_between(0, min(cap, base * 2 ** attempt))
	return rand.Intn(int(math.Min(maxMilliSeconds, float64(baseMilliSeconds)*math.Pow(2, float64(attempts)))))
}
