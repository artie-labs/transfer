package db

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"syscall"
)

var retryableErrs = []error{
	syscall.ECONNRESET,
	syscall.ECONNREFUSED,
	io.EOF,
	syscall.ETIMEDOUT,
}

// retryableHTTPStatuses contains HTTP status codes that are typically transient and retryable.
var retryableHTTPStatuses = []string{
	"HTTP Status: 429", // Too Many Requests
	"HTTP Status: 502", // Bad Gateway
	"HTTP Status: 503", // Service Unavailable
	"HTTP Status: 504", // Gateway Timeout
}

// IsRetryableError checks for common retryable errors. (example: network errors)
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	for _, retryableErr := range retryableErrs {
		if errors.Is(err, retryableErr) {
			return true
		}
	}

	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			slog.Warn("caught a net.Error in isRetryableError", slog.Any("err", err))
			return true
		}
	}

	// Check for retryable HTTP status codes (e.g., from database drivers like gosnowflake)
	errStr := err.Error()
	for _, status := range retryableHTTPStatuses {
		if strings.Contains(errStr, status) {
			slog.Warn("caught a retryable HTTP status in isRetryableError", slog.Any("err", err), slog.String("status", status))
			return true
		}
	}

	return false
}

// backward compat
func isRetryableError(err error) bool {
	return IsRetryableError(err)
}
