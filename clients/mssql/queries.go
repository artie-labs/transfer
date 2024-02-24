package mssql

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
)

func describeTableQuery(schema, rawTableName string) string {
	return fmt.Sprintf(`
SELECT 
    COLUMN_NAME, 
    DATA_TYPE, 
    CHARACTER_MAXIMUM_LENGTH, 
    NUMERIC_PRECISION, 
    NUMERIC_SCALE, 
    IS_NULLABLE
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
