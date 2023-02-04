package bigquery

import "strings"

func ColumnAlreadyExistErr(err error) bool {
	if err == nil {
		return false
	}

	// Error ends up looking like something like this: Column already exists: _string at [1:39]
	return strings.Contains(err.Error(), "Column already exists")
}
