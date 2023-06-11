package snowflake

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/typing"
)

// addPrefixToTableName will take the fully qualified table name and add a prefix in front of the table
// This is necessary for `PUT` commands. The fq name looks like <namespace>.<tableName>
// Namespace may contain both database and schema.
func addPrefixToTableName(fqTableName string, prefix string) string {
	tableParts := strings.Split(fqTableName, ".")
	if len(tableParts) == 1 {
		return prefix + fqTableName
	}

	return fmt.Sprintf("%s.%s%s",
		strings.Join(tableParts[0:len(tableParts)-1], "."), prefix, tableParts[len(tableParts)-1])
}

// escapeColumns will take the columns that are passed in, escape them and return them in the ordered received.
// It'll return like this: $1, $2, $3
func escapeColumns(columns *columns.Columns, delimiter string) string {
	var escapedCols []string
	for index, col := range columns.GetColumnsToUpdate(nil) {
		colKind, _ := columns.GetColumn(col)
		escapedCol := fmt.Sprintf("$%d", index+1)
		switch colKind.KindDetails {
		case typing.Struct:
			// https://community.snowflake.com/s/article/how-to-load-json-values-in-a-csv-file
			escapedCol = fmt.Sprintf("PARSE_JSON(%s)", escapedCol)
		case typing.Array:
			escapedCol = fmt.Sprintf("CAST(PARSE_JSON(%s) AS ARRAY) AS %s", escapedCol, escapedCol)
		}

		escapedCols = append(escapedCols, escapedCol)
	}

	return strings.Join(escapedCols, delimiter)
}
