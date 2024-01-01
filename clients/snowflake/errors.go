package snowflake

import "strings"

func IsAuthExpiredError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "Authentication token has expired")
}
