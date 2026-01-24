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
		// Test wrapped closed network connection error
		assert.True(t, isRetryableError(fmt.Errorf("databricks: driver error: error sending http request: %w", net.ErrClosed)), "wrapped closed network connection error should be retryable")
	}
	{
		// Test string-based matching for "use of closed network connection"
		brokenChainErr := fmt.Errorf("databricks: driver error: error sending http request: Put \"https://...\": write tcp ...: use of closed network connection")
		assert.True(t, isRetryableError(brokenChainErr), "string-matched closed network connection error should be retryable")
	}
	{
		// Test string-based matching for connection reset by peer
		connResetErr := fmt.Errorf("some driver error: connection reset by peer")
		assert.True(t, isRetryableError(connResetErr), "string-matched connection reset error should be retryable")
	}
	{
		// Test Databricks execution error (driver doesn't expose underlying network error)
		databricksErr := fmt.Errorf("databricks: execution error: failed to execute query")
		assert.True(t, isRetryableError(databricksErr), "Databricks execution error should be retryable")
	}
	{
		// Test wrapped Databricks execution error
		wrappedErr := fmt.Errorf("failed to run PUT INTO for temporary table: databricks: execution error: failed to execute query")
		assert.True(t, isRetryableError(wrappedErr), "wrapped Databricks execution error should be retryable")
	}
}
