package db

import (
	"fmt"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRetryable_Errors(t *testing.T) {
	{
		// Test nil error case
		var err error
		assert.False(t, isRetryableError(err), "nil error should not be retryable")
	}
	{
		// Test irrelevant error case
		assert.False(t, isRetryableError(fmt.Errorf("random error")), "irrelevant error should not be retryable")
	}
	{
		// Test direct connection refused error
		assert.True(t, isRetryableError(syscall.ECONNREFUSED), "direct connection refused error should be retryable")
	}
	{
		// Test direct connection reset error
		assert.True(t, isRetryableError(syscall.ECONNRESET), "direct connection reset error should be retryable")
	}
	{
		// Test wrapped connection refused error
		assert.True(t, isRetryableError(fmt.Errorf("foo: %w", syscall.ECONNREFUSED)), "wrapped connection refused error should be retryable")
	}
	{
		// Test wrapped connection reset error
		assert.True(t, isRetryableError(fmt.Errorf("foo: %w", syscall.ECONNRESET)), "wrapped connection reset error should be retryable")
	}
}
