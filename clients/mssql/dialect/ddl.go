package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
	mssql "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
)

func (MSSQLDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	mssqlTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	return `
SELECT
    COLUMN_NAME,
    CASE
        WHEN DATA_TYPE = 'numeric' THEN
		'numeric(' + COALESCE(CAST(NUMERIC_PRECISION AS VARCHAR), '') + ',' + COALESCE(CAST(NUMERIC_SCALE AS VARCHAR), '') + ')'
		ELSE
		DATA_TYPE
	END AS DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    COLUMN_DEFAULT AS DEFAULT_VALUE
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    LOWER(TABLE_NAME) = LOWER(?) AND LOWER(TABLE_SCHEMA) = LOWER(?);`, []any{mssql.VarChar(mssqlTableID.Table()), mssql.VarChar(mssqlTableID.Schema())}, nil
}

func (md MSSQLDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return md.buildAlterColumnQuery(tableID, constants.Add, sqlPart)
}

func (md MSSQLDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return md.buildAlterColumnQuery(tableID, constants.Delete, colName)
}

func (MSSQLDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Microsoft SQL Server uses the same syntax for temporary and permanent tables.
	// Microsoft SQL Server doesn't support IF NOT EXISTS
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (MSSQLDialect) buildAlterColumnQuery(tableID sql.TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string {
	// Microsoft SQL Server doesn't support the COLUMN keyword
	return fmt.Sprintf("ALTER TABLE %s %s %s", tableID.FullyQualifiedName(), columnOp, colSQLPart)
}
