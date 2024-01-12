package parquetutil

import (
	"encoding/json"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func GenerateJSONSchema(columns []columns.Column) (string, error) {
	var fields []typing.Field
	for _, column := range columns {
		// We don't need to escape the column name here.
		field, err := column.KindDetails.ParquetAnnotation(column.Name(nil))
		if err != nil {
			return "", err
		}

		fields = append(fields, *field)
	}

	schemaBytes, err := json.Marshal(typing.Field{
		Tag: typing.FieldTag{
			Name:           "parquet-go-root",
			RepetitionType: ptr.ToString("REQUIRED"),
		}.String(),
		Fields: fields,
	})

	if err != nil {
		return "", err
	}

	return string(schemaBytes), nil
}
