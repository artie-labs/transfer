package shared

import "strings"

// SnowflakeTableDoesNotExistErr will check if the resulting error message looks like this
// Table 'DATABASE.SCHEMA.TABLE' does not exist or not authorized. (resulting error message from DESC table)
func SnowflakeTableDoesNotExistErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "does not exist or not authorized")
}
