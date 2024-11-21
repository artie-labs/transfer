package parquetutil

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type ParquetColumn struct {
	originalName string
	cleanedName  string
	column       columns.Column
}

func (pc ParquetColumn) OriginalName() string {
	return pc.originalName
}

func (pc ParquetColumn) CleanedName() string {
	return pc.cleanedName
}

func (pc ParquetColumn) Column() columns.Column {
	return pc.column
}

func NewParquetColumn(colName string, column columns.Column) ParquetColumn {
	return ParquetColumn{
		originalName: colName,
		cleanedName:  strings.TrimPrefix(colName, "__"),
		column:       column,
	}
}

func GenerateJSONSchema(columns []ParquetColumn) (string, error) {
	var fields []typing.Field
	for _, column := range columns {
		// We don't need to escape the column name here.
		field, err := column.column.KindDetails.ParquetAnnotation(column.cleanedName)
		if err != nil {
			return "", err
		}

		fields = append(fields, *field)
	}

	schemaBytes, err := json.Marshal(
		typing.Field{
			Tag:    typing.FieldTag{Name: "parquet-go-root", RepetitionType: typing.ToPtr("REQUIRED")}.String(),
			Fields: fields,
		},
	)

	fmt.Println("schemaBytes", string(schemaBytes))

	if err != nil {
		return "", err
	}

	return string(schemaBytes), nil
}

func GenerateCSVSchema(columns []columns.Column) ([]string, error) {
	var fields []string
	for _, column := range columns {
		// We don't need to escape the column name here.
		field, err := column.KindDetails.ParquetAnnotation(column.Name())
		if err != nil {
			return nil, err
		}

		fields = append(fields, field.Tag)
	}

	return fields, nil
}
