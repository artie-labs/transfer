package retry

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetryConfig_WithRetries(t *testing.T) {
	{
		// 0 max attempts - still runs
		retryCfg := NewRetryConfig(NewRetryConfigArgs{})
		calls := 0
		err := retryCfg.WithRetries(func(attempt int, _ error) error {
			calls++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, calls, 1)
	}
	{
		// 1 max attempts - succeeds
		calls := 0
		retryCfg := NewRetryConfig(NewRetryConfigArgs{MaxAttempts: 1})
		err := retryCfg.WithRetries(func(attempt int, _ error) error {
			calls++
			return nil
		})
		assert.NoError(t, err)
		assert.Equal(t, calls, 1)
	}
	{
		// 1 max attempts - fails
		calls := 0
		retryCfg := NewRetryConfig(NewRetryConfigArgs{MaxAttempts: 1})
		err := retryCfg.WithRetries(func(attempt int, _ error) error {
			calls++
			return fmt.Errorf("oops I failed again")
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 1)
	}
	{
		// 2 max attempts - first fails and second succeeds
		calls := 0
		retryCfg := NewRetryConfig(NewRetryConfigArgs{MaxAttempts: 2})
		err := retryCfg.WithRetries(func(attempt int, _ error) error {
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
		retryCfg := NewRetryConfig(NewRetryConfigArgs{
			MaxAttempts:    3,
			IsRetryableErr: func(err error) bool { return strings.Contains(err.Error(), "retry") },
		})
		err := retryCfg.WithRetries(func(attempt int, _ error) error {
			calls++
			if attempt == 0 {
				return fmt.Errorf("retry this one")
			} else if attempt == 1 {
				return fmt.Errorf("oops I failed again")
			}
			assert.Fail(t, "Should not happen")
			return nil
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 2)
	}
}

func TestWithRetries(t *testing.T) {
	{
		// 0 max attempts - still runs
		retryCfg := NewRetryConfig(NewRetryConfigArgs{})
		calls := 0
		value, err := WithRetries(retryCfg, func(attempt int, _ error) (int, error) {
			calls++
			return 100, nil
		})
		assert.NoError(t, err)
		assert.Equal(t, value, 100)
		assert.Equal(t, calls, 1)
	}
	{
		// 1 max attempts - succeeds
		calls := 0
		retryCfg := NewRetryConfig(NewRetryConfigArgs{MaxAttempts: 1})
		value, err := WithRetries(retryCfg, func(attempt int, _ error) (int, error) {
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
		retryCfg := NewRetryConfig(NewRetryConfigArgs{MaxAttempts: 1})
		_, err := WithRetries(retryCfg, func(attempt int, _ error) (int, error) {
			calls++
			return 0, fmt.Errorf("oops I failed again")
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 1)
	}
	{
		// 2 max attempts - first fails and second succeeds
		calls := 0
		retryCfg := NewRetryConfig(NewRetryConfigArgs{MaxAttempts: 2})
		value, err := WithRetries(retryCfg, func(attempt int, _ error) (int, error) {
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
		retryCfg := NewRetryConfig(NewRetryConfigArgs{
			MaxAttempts:    3,
			IsRetryableErr: func(err error) bool { return strings.Contains(err.Error(), "retry") },
		})
		_, err := WithRetries(retryCfg, func(attempt int, _ error) (int, error) {
			calls++
			if attempt == 0 {
				return 0, fmt.Errorf("retry this one")
			} else if attempt == 1 {
				return 0, fmt.Errorf("oops I failed again")
			}
			assert.Fail(t, "Should not happen")
			return 0, nil
		})
		assert.ErrorContains(t, err, "oops I failed again")
		assert.Equal(t, calls, 2)
	}
}
