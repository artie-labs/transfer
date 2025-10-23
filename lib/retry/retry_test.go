package retry

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewJitterRetryConfig(t *testing.T) {
	{
		_, err := NewJitterRetryConfig(0, 1, 1, AlwaysRetry)
		assert.ErrorContains(t, err, "jitter baseMs must be >= 0")
	}
	{
		_, err := NewJitterRetryConfig(1, 0, 1, AlwaysRetry)
		assert.ErrorContains(t, err, "jitter maxMs must be >= 0")
	}
	{
		_, err := NewJitterRetryConfig(1, 1, 0, AlwaysRetry)
		assert.ErrorContains(t, err, "maxAttempts must be >= 1")
	}
	{
		// Happy path
		_, err := NewJitterRetryConfig(1, 1, 1, AlwaysRetry)
		assert.NoError(t, err)
	}
}

func TestWithRetries(t *testing.T) {
	{
		// 1 max attempts - succeeds, no retries
		calls := 0
		retryCfg, err := NewJitterRetryConfig(1, 1, 1, AlwaysRetry)
		assert.NoError(t, err)
		err = WithRetries(retryCfg, func(attempt int, _ error) error {
			calls++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, calls, 1)
	}
	{
		// 1 max attempts - fails and shouldn't retry
		calls := 0
		retryCfg, err := NewJitterRetryConfig(1, 1, 1, AlwaysRetry)
		assert.NoError(t, err)
		err = WithRetries(retryCfg, func(attempt int, _ error) error {
			calls++
			return fmt.Errorf("oops I failed again")
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 1)
	}
	{
		// 2 max attempts - first fails and second succeeds
		calls := 0
		retryCfg, err := NewJitterRetryConfig(1, 1, 2, AlwaysRetry)
		assert.NoError(t, err)
		err = WithRetries(retryCfg, func(attempt int, _ error) error {
			calls++
			if attempt == 0 {
				return fmt.Errorf("oops I failed again")
			}
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, calls, 2)
	}
	{
		// 3 max attempts - first fails with a retryable error, second fails with a non-retryable error
		calls := 0
		retryCfg, err := NewJitterRetryConfig(1, 1, 3,
			func(err error) bool { return strings.Contains(err.Error(), "retry") },
		)
		assert.NoError(t, err)
		err = WithRetries(retryCfg, func(attempt int, _ error) error {
			calls++
			switch attempt {
			case 0:
				return fmt.Errorf("retry this one")
			case 1:
				return fmt.Errorf("oops I failed again")
			}
			assert.Fail(t, "Should not happen")
			return nil
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 2)
	}
}

func TestWithRetriesAndResult(t *testing.T) {
	{
		// 1 max attempts - succeeds and just runs once
		calls := 0
		retryCfg, err := NewJitterRetryConfig(1, 1, 1, AlwaysRetry)
		assert.NoError(t, err)
		value, err := WithRetriesAndResult(retryCfg, func(attempt int, _ error) (int, error) {
			calls++
			return 100, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, value, 100)
		assert.Equal(t, calls, 1)
	}
	{
		// 1 max attempts - fails
		calls := 0
		retryCfg, err := NewJitterRetryConfig(1, 1, 1, AlwaysRetry)
		assert.NoError(t, err)
		_, err = WithRetriesAndResult(retryCfg, func(attempt int, _ error) (int, error) {
			calls++
			return 0, fmt.Errorf("oops I failed again")
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 1)
	}
	{
		// 2 max attempts - first fails and second succeeds
		calls := 0
		retryCfg, err := NewJitterRetryConfig(1, 1, 2, AlwaysRetry)
		assert.NoError(t, err)
		value, err := WithRetriesAndResult(retryCfg, func(attempt int, _ error) (int, error) {
			calls++
			if attempt == 0 {
				return 0, fmt.Errorf("oops I failed again")
			}
			return 100, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, value, 100)
		assert.Equal(t, calls, 2)
	}
	{
		// 3 max attempts - first fails with a retryable error, second fails with a non-retryable error
		calls := 0
		retryCfg, err := NewJitterRetryConfig(1, 1, 3,
			func(err error) bool { return strings.Contains(err.Error(), "retry") },
		)
		assert.NoError(t, err)
		_, err = WithRetriesAndResult(retryCfg, func(attempt int, _ error) (int, error) {
			calls++
			switch attempt {
			case 0:
				return 0, fmt.Errorf("retry this one")
			case 1:
				return 0, fmt.Errorf("oops I failed again")
			}
			assert.Fail(t, "Should not happen")
			return 0, nil
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 2)
	}
}
