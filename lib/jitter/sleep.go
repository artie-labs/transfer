package jitter

import (
	"log/slog"
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

type RetryConfig struct {
	jitterBaseMs   int
	jitterMaxMs    int
	maxAttempts    int
	isRetryableErr func(err error) bool
}

type NewRetryConfigArgs struct {
	JitterBaseMs   int
	JitterMaxMs    int
	MaxAttempts    int
	IsRetryableErr func(err error) bool
}

func NewRetryConfig(args NewRetryConfigArgs) RetryConfig {
	isRetryableErr := args.IsRetryableErr
	if isRetryableErr == nil {
		isRetryableErr = func(_ error) bool { return true }
	}

	return RetryConfig{
		jitterBaseMs:   min(args.JitterBaseMs, 0),
		jitterMaxMs:    min(args.JitterMaxMs, 0),
		maxAttempts:    max(args.MaxAttempts, 1),
		isRetryableErr: isRetryableErr,
	}
}

func WithRetries[T any](retryCfg RetryConfig, f func(attempt int) (T, error)) (T, error) {
	var result T
	var err error
	for attempt := 0; attempt < retryCfg.maxAttempts; attempt++ {
		if attempt > 0 {
			sleepDuration := Jitter(retryCfg.jitterBaseMs, retryCfg.jitterMaxMs, attempt)
			if sleepDuration > 0 {
				slog.Info("An error occurred, retrying after delay...",
					slog.Duration("sleep", sleepDuration),
					slog.Any("attemptsLeft", retryCfg.maxAttempts-attempt),
					slog.Any("err", err),
				)
				time.Sleep(sleepDuration)
			} else {
				slog.Info("An error occurred, retrying...",
					slog.Any("attemptsLeft", retryCfg.maxAttempts-attempt),
					slog.Any("err", err),
				)
			}
		}
		result, err = f(attempt)
		if err == nil {
			return result, nil
		} else if !retryCfg.isRetryableErr(err) {
			break
		}
	}
	return result, err
}
