package mssql

import (
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
    CHARACTER_MAXIMUM_LENGTH,
	COLUMN_DEFAULT AS DEFAULT_VALUE
FROM
    INFORMATION_SCHEMA.COLUMNS
WHERE
    LOWER(TABLE_NAME) = LOWER(?) AND LOWER(TABLE_SCHEMA) = LOWER(?);`, []any{mssql.VarChar(tableID.Table()), mssql.VarChar(tableID.Schema())}
}
