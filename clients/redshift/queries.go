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
    information_schema.columns c 
LEFT JOIN 
    pg_class c1 ON c.table_name=c1.relname 
LEFT JOIN 
    pg_catalog.pg_namespace n ON c.table_schema=n.nspname AND c1.relnamespace=n.oid 
LEFT JOIN 
    pg_catalog.pg_description d ON d.objsubid=c.ordinal_position AND d.objoid=c1.oid 
WHERE 
    LOWER(c.table_name) = LOWER(?) AND LOWER(c.table_schema) = LOWER(?);
`, constants.StrPrecisionCol), []any{args.RawTableName, args.Schema}
}
