package redshift

import "strings"

func retryableError(err error) bool {
	if err != nil {
		return strings.Contains(err.Error(), "read: connection reset by peer")
	}

	return false
}
