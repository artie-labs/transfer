package parquetutil

import (
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"
)

func BuildCSVSchema(columns []columns.Column, location *time.Location) ([]string, error) {
	var fields []string
	for _, column := range columns {
		// We don't need to escape the column name here.
		field, err := column.KindDetails.ParquetAnnotation(column.Name(), location)
		if err != nil {
			return nil, err
		}

		fields = append(fields, field.Tag)
	}

	return fields, nil
}
