package dml

import (
	"fmt"
	"slices"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func quoteColumns(cols []columns.Column, dialect sql.Dialect) []string {
	result := make([]string, len(cols))
	for i, col := range cols {
		result[i] = dialect.QuoteIdentifier(col.Name())
	}
	return result
}

func removeDeleteColumnMarker(cols []columns.Column) ([]columns.Column, bool) {
	origLength := len(cols)
	// Use [slices.Clone] because [slices.DeleteFunc] mutates its inputs.
	cols = slices.DeleteFunc(slices.Clone(cols), func(col columns.Column) bool { return col.Name() == constants.DeleteColumnMarker })
	return cols, len(cols) != origLength
}

// buildColumnsUpdateFragment will parse the columns and then returns a list of strings like: cc.first_name=c.first_name,cc.last_name=c.last_name,cc.email=c.email
// NOTE: This should only be used with valid columns.
func buildColumnsUpdateFragment(columns []columns.Column, dialect sql.Dialect) string {
	var cols []string
	for _, column := range columns {
		colName := dialect.QuoteIdentifier(column.Name())
		if column.ToastColumn {
			var colValue string
			if column.KindDetails == typing.Struct {
				colValue = dialect.BuildProcessToastStructColExpression(colName)
			} else {
				colValue = processToastCol(colName, dialect)
			}
			cols = append(cols, fmt.Sprintf("%s= %s", colName, colValue))
		} else {
			// This is to make it look like: objCol = cc.objCol
			cols = append(cols, fmt.Sprintf("%s=cc.%s", colName, colName))
		}
	}

	return strings.Join(cols, ",")
}

func processToastCol(colName string, dialect sql.Dialect) string {
	if _, ok := dialect.(sql.MSSQLDialect); ok {
		// Microsoft SQL Server doesn't allow boolean expressions to be in the COALESCE statement.
		return fmt.Sprintf("CASE WHEN COALESCE(cc.%s, '') != '%s' THEN cc.%s ELSE c.%s END", colName,
			constants.ToastUnavailableValuePlaceholder, colName, colName)
	} else {
		return fmt.Sprintf("CASE WHEN COALESCE(cc.%s != '%s', true) THEN cc.%s ELSE c.%s END",
			colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	}
}
