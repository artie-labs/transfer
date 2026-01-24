package db

import (
	"errors"
	"fmt"
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
	net.ErrClosed, // "use of closed network connection" - connection closed during idle period
}

// retryableErrStrings contains error substrings that indicate retryable errors.
// This is needed because some drivers (like Databricks) wrap errors with fmt.Errorf("%v", err)
// instead of fmt.Errorf("%w", err), breaking the error chain for errors.Is().
var retryableErrStrings = []string{
	"use of closed network connection", // Connection closed during idle period
	"connection reset by peer",         // Remote end closed connection
}

// IsRetryableError checks for common retryable errors. (example: network errors)
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Temporarily log at INFO level to debug retry issues
	errMsg := err.Error()
	slog.Info("IsRetryableError checking error", slog.String("errMsg", errMsg), slog.String("errType", fmt.Sprintf("%T", err)))

	for _, retryableErr := range retryableErrs {
		if errors.Is(err, retryableErr) {
			slog.Info("IsRetryableError matched via errors.Is", slog.Any("matched", retryableErr))
			return true
		}
	}

	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			slog.Warn("caught a net.Error in isRetryableError", slog.Any("err", err))
			return true
		}
	}

	// Fallback to string matching for errors that don't properly wrap the underlying error.
	// This handles cases where third-party drivers use %v instead of %w in fmt.Errorf.
	errMsgLower := strings.ToLower(errMsg)
	for _, retryableStr := range retryableErrStrings {
		if strings.Contains(errMsgLower, retryableStr) {
			slog.Warn("caught retryable error via string match", slog.Any("err", err), slog.String("matched", retryableStr))
			return true
		}
	}

	slog.Info("IsRetryableError: no match found", slog.String("errMsg", errMsg))
	return false
}

// backward compat
func isRetryableError(err error) bool {
	return IsRetryableError(err)
}
