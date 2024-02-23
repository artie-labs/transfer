package mssql

import (
	"fmt"
	"strings"
)

type describeArgs struct {
	RawTableName string
	Schema       string
}

func describeTableQuery(args describeArgs) (string, error) {
	if strings.Contains(args.RawTableName, `"`) {
		return "", fmt.Errorf("table name cannot contain double quotes")
	}

	// This query is a modified fork from: https://gist.github.com/alexanderlz/7302623
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
    LOWER(table_name) = LOWER('%s') AND LOWER(table_schema) = LOWER('%s');
`, args.RawTableName, args.Schema), nil
}
