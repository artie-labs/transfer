package db

import (
	"errors"
	"io"
	"syscall"
)

var retryableErrs = []error{
	syscall.ECONNRESET,
	syscall.ECONNREFUSED,
	io.EOF,
}

func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	for _, retryableErr := range retryableErrs {
		if errors.Is(err, retryableErr) {
			return true
		}
	}

	return false
}
