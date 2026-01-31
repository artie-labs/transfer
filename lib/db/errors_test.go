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

func TestIsRetryable_HTTPStatusErrors(t *testing.T) {
	{
		// Test HTTP 429 Too Many Requests
		err := fmt.Errorf("timeout after 0s and 8 attempts. HTTP Status: 429")
		assert.True(t, isRetryableError(err), "HTTP 429 error should be retryable")
	}
	{
		// Test HTTP 502 Bad Gateway
		err := fmt.Errorf("timeout after 0s and 8 attempts. HTTP Status: 502")
		assert.True(t, isRetryableError(err), "HTTP 502 error should be retryable")
	}
	{
		// Test HTTP 503 Service Unavailable (Snowflake-style error)
		err := fmt.Errorf("timeout after 0s and 8 attempts. HTTP Status: 503. Hanging?")
		assert.True(t, isRetryableError(err), "HTTP 503 error should be retryable")
	}
	{
		// Test HTTP 504 Gateway Timeout
		err := fmt.Errorf("timeout after 0s and 8 attempts. HTTP Status: 504")
		assert.True(t, isRetryableError(err), "HTTP 504 error should be retryable")
	}
	{
		// Test HTTP 400 Bad Request (not retryable)
		err := fmt.Errorf("HTTP Status: 400 Bad Request")
		assert.False(t, isRetryableError(err), "HTTP 400 error should not be retryable")
	}
	{
		// Test HTTP 500 Internal Server Error (not in our retry list by default)
		err := fmt.Errorf("HTTP Status: 500 Internal Server Error")
		assert.False(t, isRetryableError(err), "HTTP 500 error should not be retryable by default")
	}
}
