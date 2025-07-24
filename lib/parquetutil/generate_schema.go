package parquetutil

import (
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// BuildArrowSchemaFromColumns creates an Arrow schema from typing columns
func BuildArrowSchemaFromColumns(columns []columns.Column, location *time.Location) (*arrow.Schema, error) {
	var fields []arrow.Field
	for _, column := range columns {
		arrowType, err := column.KindDetails.ToArrowType(location)
		if err != nil {
			return nil, err
		}

		fields = append(fields, arrow.Field{
			Name:     column.Name(),
			Type:     arrowType,
			Nullable: true,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}
