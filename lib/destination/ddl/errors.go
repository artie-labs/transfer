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
	case constants.MSSQL:
		alreadyExistErrs := []string{
			// Column names in each table must be unique. Column name 'first_name' in table 'users' is specified more than once.
			"Column names in each table must be unique",
			// There is already an object named 'customers' in the database.
			"There is already an object named",
		}

		for _, alreadyExistErr := range alreadyExistErrs {
			if alreadyExist := strings.Contains(err.Error(), alreadyExistErr); alreadyExist {
				return alreadyExist
			}
		}
	}

	return false
}
