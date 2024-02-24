package mssql

import (
	"fmt"
)

type describeArgs struct {
	RawTableName string
	Schema       string
}

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
    LOWER(table_name) = LOWER('%s') AND LOWER(table_schema) = LOWER('%s');`, rawTableName, schema)
}
