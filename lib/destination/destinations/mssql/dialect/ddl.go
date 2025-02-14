package dialect

import (
	"fmt"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
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

func (MSSQLDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD %s", tableID.FullyQualifiedName(), sqlPart)
}

func (MSSQLDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP %s", tableID.FullyQualifiedName(), colName)
}

func (MSSQLDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Microsoft SQL Server uses the same syntax for temporary and permanent tables.
	// Microsoft SQL Server doesn't support IF NOT EXISTS
	return fmt.Sprintf("CREATE TABLE %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (MSSQLDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return "DROP TABLE IF EXISTS " + tableID.FullyQualifiedName()
}

func (MSSQLDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return "TRUNCATE TABLE " + tableID.FullyQualifiedName()
}
