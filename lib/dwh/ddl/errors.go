package ddl

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"strings"
)

func ColumnAlreadyExistErr(err error, kind constants.DestinationKind) bool {
	if err == nil {
		return false
	}

	switch kind {
	case constants.BigQuery:
		// Error ends up looking like something like this: Column already exists: _string at [1:39]
		return strings.Contains(err.Error(), "Column already exists")

	case constants.Snowflake:
		// Snowflake doesn't have column mutations (IF NOT EXISTS)
		return strings.Contains(err.Error(), "already exists")
	}

	return false
}
