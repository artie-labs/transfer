package db

import "strings"

func retryableError(err error) bool {
	if err != nil {
		if strings.Contains(err.Error(), "read: connection reset by peer") {
			return true
		} else if strings.Contains(err.Error(), "connect: connection refused") {
			return true
		}
	}

	return false
}
