package dml

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// UpdateQuery will parse the columns and then returns a list of strings like: cc.first_name=c.first_name,cc.last_name=c.last_name,cc.email=c.email
func UpdateQuery(c *columns.Columns, dialect sql.Dialect, skipDeleteCol bool) string {
	var cols []string
	for _, column := range c.GetColumns() {
		if column.ShouldSkip() {
			continue
		}

		// skipDeleteCol is useful because we don't want to copy the deleted column over to the source table if we're doing a hard row delete.
		if skipDeleteCol && column.Name() == constants.DeleteColumnMarker {
			continue
		}

		colName := dialect.QuoteIdentifier(column.Name())
		if column.ToastColumn {
			if column.KindDetails == typing.Struct {
				cols = append(cols, processToastStructCol(colName, dialect))
			} else {
				cols = append(cols, processToastCol(colName, dialect))
			}

		} else {
			// This is to make it look like: objCol = cc.objCol
			cols = append(cols, fmt.Sprintf("%s=cc.%s", colName, colName))
		}
	}

	return strings.Join(cols, ",")
}

func processToastStructCol(colName string, dialect sql.Dialect) string {
	switch dialect.(type) {
	case sql.BigQueryDialect:
		return fmt.Sprintf(`%s= CASE WHEN COALESCE(TO_JSON_STRING(cc.%s) != '{"key":"%s"}', true) THEN cc.%s ELSE c.%s END`,
			colName, colName, constants.ToastUnavailableValuePlaceholder,
			colName, colName)
	case sql.RedshiftDialect:
		return fmt.Sprintf(`%s= CASE WHEN COALESCE(cc.%s != JSON_PARSE('{"key":"%s"}'), true) THEN cc.%s ELSE c.%s END`,
			colName, colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	case sql.MSSQLDialect:
		// Microsoft SQL Server doesn't allow boolean expressions to be in the COALESCE statement.
		return fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s, {}) != {'key': '%s'} THEN cc.%s ELSE c.%s END",
			colName, colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	default:
		// TODO: Change this to Snowflake and error out if the destKind isn't supported so we're explicit.
		return fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s != {'key': '%s'}, true) THEN cc.%s ELSE c.%s END",
			colName, colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	}
}

func processToastCol(colName string, dialect sql.Dialect) string {
	if _, ok := dialect.(sql.MSSQLDialect); ok {
		// Microsoft SQL Server doesn't allow boolean expressions to be in the COALESCE statement.
		return fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s, '') != '%s' THEN cc.%s ELSE c.%s END", colName, colName,
			constants.ToastUnavailableValuePlaceholder, colName, colName)
	} else {
		return fmt.Sprintf("%s= CASE WHEN COALESCE(cc.%s != '%s', true) THEN cc.%s ELSE c.%s END",
			colName, colName, constants.ToastUnavailableValuePlaceholder, colName, colName)
	}
}
