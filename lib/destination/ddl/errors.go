package ddl

import (
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func ColumnAlreadyExistErr(err error, kind constants.DestinationKind) bool {
	if err == nil {
		return false
	}

	switch kind {
	case constants.BigQuery:
		// Error ends up looking like something like this: Column already exists: _string at [1:39]
		return strings.Contains(err.Error(), "Column already exists")

	case constants.Snowflake, constants.Redshift:
		// Snowflake doesn't have column mutations (IF NOT EXISTS)
		// Redshift's error: ERROR: column "foo" of relation "statement" already exists
		return strings.Contains(err.Error(), "already exists")
	}

	return false
}
