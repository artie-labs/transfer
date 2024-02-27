package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
)

type describeArgs struct {
	RawTableName string
	Schema       string
}

func describeTableQuery(args describeArgs) (string, []any) {
	// This query is a modified fork from: https://gist.github.com/alexanderlz/7302623
	return fmt.Sprintf(`
SELECT 
    c.column_name,
    CASE 
        WHEN c.data_type = 'numeric' THEN 
            'numeric(' || COALESCE(CAST(c.numeric_precision AS VARCHAR), '') || ',' || COALESCE(CAST(c.numeric_scale AS VARCHAR), '') || ')'
        ELSE 
            c.data_type
    END AS data_type,
    c.%s,
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
    LOWER(c.table_name) = LOWER($1) AND LOWER(c.table_schema) = LOWER($2);
`, constants.StrPrecisionCol), []any{args.RawTableName, args.Schema}
}
