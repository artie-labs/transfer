package parquetutil

import (
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// BuildArrowSchemaFromColumns creates an Arrow schema from typing columns
func BuildArrowSchemaFromColumns(columns []columns.Column, location *time.Location) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(columns))

	for i, column := range columns {
		arrowType, err := column.KindDetails.ToArrowType(location)
		if err != nil {
			return nil, err
		}

		fields[i] = arrow.Field{
			Name:     column.Name(),
			Type:     arrowType,
			Nullable: true, // Most columns are nullable by default
		}
	}

	return arrow.NewSchema(fields, nil), nil
}
