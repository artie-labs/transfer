package db

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"syscall"
)

var retryableErrs = []error{
	syscall.ECONNRESET,
	syscall.ECONNREFUSED,
	io.EOF,
	syscall.ETIMEDOUT,
	net.ErrClosed, // "use of closed network connection" - connection closed during idle period
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

	return false
}

// backward compat
func isRetryableError(err error) bool {
	return IsRetryableError(err)
}
