package retry

import (
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
)

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

func (r RetryConfig) sleepIfNecessary(attempt int, err error) {
	if attempt > 0 {
		sleepDuration := jitter.Jitter(r.jitterBaseMs, r.jitterMaxMs, attempt)
		if sleepDuration > 0 {
			slog.Info("An error occurred, retrying after delay...",
				slog.Duration("sleep", sleepDuration),
				slog.Any("attemptsLeft", r.maxAttempts-attempt),
				slog.Any("err", err),
			)
			time.Sleep(sleepDuration)
		} else {
			slog.Info("An error occurred, retrying...",
				slog.Any("attemptsLeft", r.maxAttempts-attempt),
				slog.Any("err", err),
			)
		}
	}
}

func (r RetryConfig) WithRetries(f func(attempt int, err error) error) error {
	var err error
	for attempt := 0; attempt < r.maxAttempts; attempt++ {
		r.sleepIfNecessary(attempt, err)
		err = f(attempt, err)
		if err == nil {
			return nil
		} else if !r.isRetryableErr(err) {
			break
		}
	}
	return err
}

func WithRetries[T any](retryCfg RetryConfig, f func(attempt int, err error) (T, error)) (T, error) {
	var result T
	var err error
	for attempt := 0; attempt < retryCfg.maxAttempts; attempt++ {
		retryCfg.sleepIfNecessary(attempt, err)
		result, err = f(attempt, err)
		if err == nil {
			return result, nil
		} else if !retryCfg.isRetryableErr(err) {
			break
		}
	}
	return result, err
}
