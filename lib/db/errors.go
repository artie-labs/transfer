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
	net.ErrClosed, // "use of closed network connection" - connection closed during idle period
}

// retryableErrStrings contains error substrings that indicate retryable errors.
var retryableErrStrings = []string{
	"use of closed network connection",                // Connection closed during idle period
	"connection reset by peer",                        // Remote end closed connection
	"databricks: execution error: failed to execute query", // Databricks staging operation failed (likely network issue)
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

	errMsgLower := strings.ToLower(err.Error())
	for _, retryableStr := range retryableErrStrings {
		if strings.Contains(errMsgLower, retryableStr) {
			slog.Warn("caught retryable error via string match", slog.String("matched", retryableStr), slog.Any("err", err))
			return true
		}
	}

	return false
}

// backward compat
func isRetryableError(err error) bool {
	return IsRetryableError(err)
}
