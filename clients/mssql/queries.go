package mssql

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	mssql "github.com/microsoft/go-mssqldb"
)

func describeTableQuery(tableID TableIdentifier) (string, []any) {
	return `
SELECT
    COLUMN_NAME,
    CASE
        WHEN DATA_TYPE = 'numeric' THEN
		'numeric(' + COALESCE(CAST(NUMERIC_PRECISION AS VARCHAR), '') + ',' + COALESCE(CAST(NUMERIC_SCALE AS VARCHAR), '') + ')'
		ELSE
		DATA_TYPE
	END AS DATA_TYPE,
    COLUMN_DEFAULT,
    CHARACTER_MAXIMUM_LENGTH
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    LOWER(TABLE_NAME) = LOWER(?) AND LOWER(TABLE_SCHEMA) = LOWER(?);`, []any{mssql.VarChar(tableID.Table()), mssql.VarChar(tableID.Schema())}
}

func sweepQuery(schema string) (string, []any) {
	return `
SELECT
    TABLE_SCHEMA, TABLE_NAME
FROM
    INFORMATION_SCHEMA.TABLES
WHERE
    LOWER(TABLE_NAME) LIKE ? AND LOWER(TABLE_SCHEMA) = LOWER(?)`, []any{mssql.VarChar("%" + constants.ArtiePrefix + "%"), mssql.VarChar(schema)}
}
