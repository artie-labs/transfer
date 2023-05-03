package jitter

import (
	"golang.org/x/exp/rand"
	"math"
)

const maxSeconds = 20

func Jitter(baseSeconds, attempts int) int {
	// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
	// sleep = random_between(0, min(cap, base * 2 ** attempt))
	return rand.Intn(int(math.Min(maxSeconds, float64(baseSeconds)*math.Pow(2, float64(attempts)))))
}
