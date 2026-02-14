package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

const describeTableQuery = `
SELECT
    COLUMN_NAME,
    CASE
        WHEN DATA_TYPE IN ('decimal', 'numeric') THEN
            CONCAT(DATA_TYPE, '(', NUMERIC_PRECISION, ',', NUMERIC_SCALE, ')')
        WHEN DATA_TYPE IN ('varchar', 'char', 'varbinary', 'binary') THEN
            CONCAT(DATA_TYPE, '(', CHARACTER_MAXIMUM_LENGTH, ')')
        WHEN DATA_TYPE IN ('datetime', 'timestamp', 'time') AND DATETIME_PRECISION > 0 THEN
            CONCAT(DATA_TYPE, '(', DATETIME_PRECISION, ')')
        WHEN DATA_TYPE = 'tinyint' AND COLUMN_TYPE = 'tinyint(1)' THEN
            'boolean'
        ELSE
            DATA_TYPE
    END AS DATA_TYPE,
    COLUMN_DEFAULT AS DEFAULT_VALUE
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    LOWER(TABLE_SCHEMA) = LOWER(?) AND LOWER(TABLE_NAME) = LOWER(?);
`

func (MySQLDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	mysqlTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	return describeTableQuery, []any{mysqlTableID.Database(), mysqlTableID.Table()}, nil
}

func (MySQLDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return sql.DefaultBuildAddColumnQuery(tableID, sqlPart)
}

func (MySQLDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return sql.DefaultBuildDropColumnQuery(tableID, colName)
}

func (MySQLDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, _ config.Mode, colSQLParts []string) string {
	// MySQL uses the same syntax for temporary and permanent tables.
	// We don't use TEMPORARY keyword because we use connection pooling.
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (MySQLDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildDropTableQuery(tableID)
}

func (MySQLDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return sql.DefaultBuildTruncateTableQuery(tableID)
}
