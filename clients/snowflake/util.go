package snowflake

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// addPrefixToTableName will take a [types.TableIdentifier] and add a prefix in front of the table
// This is necessary for `PUT` commands. The fq name looks like <namespace>.<tableName>
// Namespace may contain both database and schema.
func addPrefixToTableName(tableID types.TableIdentifier, prefix string) string {
	return tableID.WithTable(prefix + tableID.Table()).FullyQualifiedName()
}

// escapeColumns will take columns, filter out invalid, escape and return them in ordered received.
// It'll return like this: $1, $2, $3
func escapeColumns(columns *columns.Columns, delimiter string) string {
	var escapedCols []string
	var index int
	for _, col := range columns.GetColumns() {
		escapedCol := fmt.Sprintf("$%d", index+1)
		switch col.KindDetails {
		case typing.Invalid:
			continue
		case typing.Struct:
			// https://community.snowflake.com/s/article/how-to-load-json-values-in-a-csv-file
			escapedCol = fmt.Sprintf("PARSE_JSON(%s)", escapedCol)
		case typing.Array:
			escapedCol = fmt.Sprintf("CAST(PARSE_JSON(%s) AS ARRAY) AS %s", escapedCol, escapedCol)
		}

		escapedCols = append(escapedCols, escapedCol)
		index += 1
	}

	return strings.Join(escapedCols, delimiter)
}
