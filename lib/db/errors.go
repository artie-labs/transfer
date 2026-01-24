package db

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"syscall"

	pkgerrors "github.com/pkg/errors"
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

// containsRetryableMessage checks if the error message contains any retryable string.
func containsRetryableMessage(errMsg string) (bool, string) {
	errMsgLower := strings.ToLower(errMsg)
	for _, retryableStr := range retryableErrStrings {
		if strings.Contains(errMsgLower, retryableStr) {
			return true, retryableStr
		}
	}
	return false, ""
}

// IsRetryableError checks for common retryable errors. (example: network errors)
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

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

	// Traverse the error chain checking each message for retryable patterns.
	// This handles cases where error types (like Databricks executionError) don't
	// include the cause message in their Error() output.
	for e := err; e != nil; {
		if matched, pattern := containsRetryableMessage(e.Error()); matched {
			slog.Warn("caught retryable error via string match", slog.String("matched", pattern), slog.Any("err", err))
			return true
		}

		// Try standard unwrapping (Go 1.13+)
		if unwrapped := errors.Unwrap(e); unwrapped != nil {
			e = unwrapped
			continue
		}

		// Try github.com/pkg/errors
		if unwrapped := pkgerrors.Unwrap(e); unwrapped != nil {
			e = unwrapped
			continue
		}

		// Fallback: Cause() method (used by Databricks driver's custom error types)
		if c, ok := e.(interface{ Cause() error }); ok {
			if cause := c.Cause(); cause != nil {
				e = cause
				continue
			}
		}

		break
	}

	return false
}

// backward compat
func isRetryableError(err error) bool {
	return IsRetryableError(err)
}
