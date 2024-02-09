package db

import (
	"errors"
	"log/slog"
	"strings"
	"syscall"
)

func retryableError(err error) bool {
	if err != nil {
		if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
			return true
		}
		if strings.Contains(err.Error(), "read: connection reset by peer") {
			// TODO: Remove this conditional if we don't see this warning in logs
			slog.Warn("matched 'read: connection reset by peer' by string")
			return true
		} else if strings.Contains(err.Error(), "connect: connection refused") {
			// TODO: Remove this conditional if we don't see this warning in logs
			slog.Warn("matched 'connect: connection refused' by string")
			return true
		}
	}

	return false
}
