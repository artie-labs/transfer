package db

import (
	"fmt"
	"net"
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
	{
		// Test direct closed network connection error
		assert.True(t, isRetryableError(net.ErrClosed), "direct closed network connection error should be retryable")
	}
	{
		// Test wrapped closed network connection error (simulates "use of closed network connection" from HTTP client)
		assert.True(t, isRetryableError(fmt.Errorf("databricks: driver error: error sending http request: %w", net.ErrClosed)), "wrapped closed network connection error should be retryable")
	}
	{
		// Test string-based matching for errors that break the error chain (using %v instead of %w)
		// This simulates how the Databricks driver wraps errors
		brokenChainErr := fmt.Errorf("databricks: driver error: error sending http request: Put \"https://...\": write tcp ...: use of closed network connection")
		assert.True(t, isRetryableError(brokenChainErr), "string-matched closed network connection error should be retryable")
	}
	{
		// Test string-based matching for connection reset by peer
		connResetErr := fmt.Errorf("some driver error: connection reset by peer")
		assert.True(t, isRetryableError(connResetErr), "string-matched connection reset error should be retryable")
	}
}
