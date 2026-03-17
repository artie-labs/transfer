package db

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"slices"
	"syscall"
)

var retryableErrs = []error{
	syscall.ECONNRESET,
	syscall.ECONNREFUSED,
	io.EOF,
	syscall.ETIMEDOUT,
}

// IsRetryableError checks for common retryable errors. (example: network errors)
func IsRetryableError(err error, additionalRetryableErrs ...error) bool {
	if err == nil {
		return false
	}

	for _, retryableErr := range slices.Concat(retryableErrs, additionalRetryableErrs) {
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

	return false
}

// backward compat
func isRetryableError(err error) bool {
	return IsRetryableError(err)
}
