package snowflake

import (
	"github.com/artie-labs/transfer/lib/sql"
)

// addPrefixToTableName will take a [sql.TableIdentifier] and add a prefix in front of the table.
// This is necessary for `PUT` commands.
func addPrefixToTableName(tableID sql.TableIdentifier, prefix string) string {
	return tableID.WithTable(prefix + tableID.Table()).FullyQualifiedName()
}
