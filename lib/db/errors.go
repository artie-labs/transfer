package db

import (
	"errors"
	"syscall"
)

func retryableError(err error) bool {
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}

	return false
}
