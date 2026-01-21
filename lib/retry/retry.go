package retry

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
)

func AlwaysRetry(_ error) bool { return true }

type RetryConfig interface {
	MaxAttempts() int
	SleepDuration(attempt int) time.Duration
	IsRetryableErr(err error) bool
}

type jitterRetryConfig struct {
	jitterBaseMs   int
	jitterMaxMs    int
	maxAttempts    int
	isRetryableErr func(err error) bool
}

func (j jitterRetryConfig) MaxAttempts() int {
	return j.maxAttempts
}

func (j jitterRetryConfig) SleepDuration(attempt int) time.Duration {
	return jitter.Jitter(j.jitterBaseMs, j.jitterMaxMs, attempt-1)
}

func (j jitterRetryConfig) IsRetryableErr(err error) bool {
	return j.isRetryableErr(err)
}

func NewJitterRetryConfig(baseMs, maxMs, maxAttempts int, isRetryableErr func(err error) bool) (RetryConfig, error) {
	if baseMs <= 0 {
		return nil, fmt.Errorf("jitter baseMs must be >= 0")
	} else if maxMs <= 0 {
		return nil, fmt.Errorf("jitter maxMs must be >= 0")
	} else if maxAttempts < 1 {
		return nil, fmt.Errorf("maxAttempts must be >= 1")
	}

	return &jitterRetryConfig{
		jitterBaseMs:   baseMs,
		jitterMaxMs:    maxMs,
		maxAttempts:    maxAttempts,
		isRetryableErr: isRetryableErr,
	}, nil
}

func sleepIfNecessary(cfg RetryConfig, attempt int, err error) {
	if attempt > 0 {
		sleepDuration := cfg.SleepDuration(attempt)
		slog.Warn("An error occurred, retrying...",
			slog.Duration("delay", sleepDuration),
			slog.Int("attemptsLeft", cfg.MaxAttempts()-attempt),
			slog.Any("err", err),
		)
		time.Sleep(sleepDuration)
	}
}

// WithRetries runs function `f` and returns the error if one occurs.
func WithRetries(cfg RetryConfig, f func(attempt int, err error) error) error {
	var prevErr error
	var err error
	for attempt := 0; attempt < cfg.MaxAttempts(); attempt++ {
		sleepIfNecessary(cfg, attempt, err)
		err = f(attempt, err)
		if err == nil {
			if attempt > 0 {
				// Only log if there was more than one attempt, so it's less noisy.
				slog.Info("Retry was successful", slog.Int("attempts", attempt), slog.Any("prevErr", prevErr))
			}

			return nil
		} else if !cfg.IsRetryableErr(err) {
			break
		}

		prevErr = err
	}

	return err
}

// WithRetriesAndResult runs function `f` and returns the result + the error if one occurs.
func WithRetriesAndResult[T any](cfg RetryConfig, f func(attempt int, err error) (T, error)) (T, error) {
	var result T
	var prevErr error
	var err error
	for attempt := 0; attempt < cfg.MaxAttempts(); attempt++ {
		sleepIfNecessary(cfg, attempt, err)
		result, err = f(attempt, err)
		if err == nil {
			if attempt > 0 {
				// Only log if there was more than one attempt, so it's less noisy.
				slog.Info("Retry was successful", slog.Int("attempts", attempt), slog.Any("prevErr", prevErr))
			}

			return result, nil
		} else if !cfg.IsRetryableErr(err) {
			break
		}

		prevErr = err
	}
	return result, err
}
