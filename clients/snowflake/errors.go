package snowflake

import "strings"

func AuthenticationExpirationErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "Authentication token has expired")
}
