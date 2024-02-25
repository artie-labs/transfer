package mssql

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func describeTableQuery(schema, rawTableName string) string {
	return fmt.Sprintf(`
SELECT 
    COLUMN_NAME, 
    CASE
        WHEN DATA_TYPE = 'numeric' THEN
		'numeric(' + COALESCE(CAST(NUMERIC_PRECISION AS VARCHAR), '') + ',' + COALESCE(CAST(NUMERIC_SCALE AS VARCHAR), '') + ')'
		ELSE
		DATA_TYPE
	END AS DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH
FROM 
    INFORMATION_SCHEMA.COLUMNS
WHERE 
    LOWER(TABLE_NAME) = LOWER('%s') AND LOWER(TABLE_SCHEMA) = LOWER('%s');`, rawTableName, schema)
}

func sweepQuery(schema string) string {
	return fmt.Sprintf(`
SELECT
	TABLE_NAME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	LOWER(TABLE_NAME) LIKE '%s' AND LOWER(TABLE_SCHEMA) = LOWER('%s')`, "%"+constants.ArtiePrefix+"%", schema)
}
