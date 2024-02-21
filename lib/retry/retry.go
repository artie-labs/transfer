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

func WithRetries[T any](retryCfg RetryConfig, f func(attempt int) (T, error)) (T, error) {
	var result T
	var err error
	for attempt := 0; attempt < retryCfg.maxAttempts; attempt++ {
		if attempt > 0 {
			sleepDuration := jitter.Jitter(retryCfg.jitterBaseMs, retryCfg.jitterMaxMs, attempt)
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
