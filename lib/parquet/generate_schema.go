package parquet

import "github.com/artie-labs/transfer/lib/typing/columns"

func GenerateJSONSchema(columns []*columns.Column) (string, error) {
	fields := make([]map[string]interface{}, len(columns))
	for i, column := range columns {
		column.KindDetails.Kind

	}
}
