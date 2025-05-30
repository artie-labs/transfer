package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
)

const describeTableQuery = `SELECT
    c.column_name,
    CASE
        WHEN c.data_type IN ('numeric') THEN
            c.data_type + '(' + CAST(c.numeric_precision AS VARCHAR) + ',' + CAST(c.numeric_scale AS VARCHAR) + ')'
        WHEN c.data_type IN ('character varying') THEN
            c.data_type + '(' + CAST(c.character_maximum_length AS VARCHAR) + ')'
        ELSE
            c.data_type
    END AS data_type,
    d.description
FROM
    INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN
    PG_CLASS c1 ON c.table_name = c1.relname
LEFT JOIN
    PG_CATALOG.PG_NAMESPACE n ON c.table_schema = n.nspname AND c1.relnamespace = n.oid
LEFT JOIN
    PG_CATALOG.PG_DESCRIPTION d ON d.objsubid = c.ordinal_position AND d.objoid = c1.oid
WHERE
    LOWER(c.table_schema) = LOWER($1) AND LOWER(c.table_name) = LOWER($2);`

func (RedshiftDialect) BuildDescribeTableQuery(tableID sql.TableIdentifier) (string, []any, error) {
	redshiftTableID, err := typing.AssertType[TableIdentifier](tableID)
	if err != nil {
		return "", nil, err
	}

	// This query is a modified fork from: https://gist.github.com/alexanderlz/7302623
	return describeTableQuery, []any{redshiftTableID.Schema(), redshiftTableID.Table()}, nil
}

func (RedshiftDialect) BuildAddColumnQuery(tableID sql.TableIdentifier, sqlPart string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableID.FullyQualifiedName(), sqlPart)
}

func (RedshiftDialect) BuildDropColumnQuery(tableID sql.TableIdentifier, colName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", tableID.FullyQualifiedName(), colName)
}

func (RedshiftDialect) BuildCreateTableQuery(tableID sql.TableIdentifier, _ bool, colSQLParts []string) string {
	// Redshift uses the same syntax for temporary and permanent tables.
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", tableID.FullyQualifiedName(), strings.Join(colSQLParts, ","))
}

func (RedshiftDialect) BuildDropTableQuery(tableID sql.TableIdentifier) string {
	return "DROP TABLE IF EXISTS " + tableID.FullyQualifiedName()
}

func (RedshiftDialect) BuildTruncateTableQuery(tableID sql.TableIdentifier) string {
	return "TRUNCATE TABLE " + tableID.FullyQualifiedName()
}
