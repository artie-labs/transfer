package typing

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"
)

type FieldTag struct {
	Name               string
	InName             *string
	Type               *string
	ConvertedType      *string
	ValueConvertedType *string
	// https://github.com/xitongsys/parquet-go#repetition-type
	RepetitionType *string
}

func (f FieldTag) String() string {
	parts := []string{
		fmt.Sprintf("name=%s", f.Name),
	}

	if f.InName != nil {
		parts = append(parts, fmt.Sprintf("inname=%s", *f.InName))
	}

	if f.Type != nil {
		parts = append(parts, fmt.Sprintf("type=%s", *f.Type))
	}

	if f.ConvertedType != nil {
		parts = append(parts, fmt.Sprintf("convertedtype=%s", *f.ConvertedType))
	}

	if f.ValueConvertedType != nil {
		parts = append(parts, fmt.Sprintf("valueconvertedtype=%s", *f.ValueConvertedType))
	}

	if f.RepetitionType != nil {
		parts = append(parts, fmt.Sprintf("repetitiontype=%s", *f.RepetitionType))
	} else {
		parts = append(parts, "repetitiontype=OPTIONAL")
	}

	return strings.Join(parts, ", ")
}

type Field struct {
	Tag    string  `json:"Tag"`
	Fields []Field `json:"Fields,omitempty"`
}

func (k *KindDetails) ParquetAnnotation(colName string) (*Field, error) {
	switch k.Kind {
	case Float.Kind:
		return &Field{
			Tag: FieldTag{
				Name:   colName,
				InName: &colName,
				Type:   ptr.ToString("FLOAT"),
			}.String(),
		}, nil
	case Integer.Kind, ETime.Kind:
		// Parquet doesn't have native time types, so we are using int64 and casting the value as UNIX ts.
		return &Field{
			Tag: FieldTag{
				Name:   colName,
				InName: &colName,
				Type:   ptr.ToString("INT64"),
			}.String(),
		}, nil
	case EDecimal.Kind:
		// TODO: Support precision and scale.
		return &Field{
			Tag: FieldTag{
				Name:   colName,
				InName: &colName,
				Type:   ptr.ToString("INT64"),
			}.String(),
		}, nil
	case Boolean.Kind:
		return &Field{
			Tag: FieldTag{
				Name:   colName,
				InName: &colName,
				Type:   ptr.ToString("BOOLEAN"),
			}.String(),
		}, nil
	case Array.Kind:
		return &Field{
			Tag: FieldTag{
				Name:           colName,
				InName:         &colName,
				Type:           ptr.ToString("LIST"),
				RepetitionType: ptr.ToString("REQUIRED"),
			}.String(),
			Fields: []Field{
				{
					Tag: FieldTag{
						Name:           "element",
						Type:           ptr.ToString("BYTE_ARRAY"),
						ConvertedType:  ptr.ToString("UTF8"),
						RepetitionType: ptr.ToString("REQUIRED"),
					}.String(),
				},
			},
		}, nil
	case String.Kind, Struct.Kind:
		// We could go further with struct, but it's very possible that it has inconsistent column headers across all the rows.
		// It's much safer to just treat this as a string. When we do bring this data out into another destination,
		// then just parse it as a JSON string, into a VARIANT column.
		return &Field{
			Tag: FieldTag{
				Name:          colName,
				InName:        &colName,
				Type:          ptr.ToString("BYTE_ARRAY"),
				ConvertedType: ptr.ToString("UTF8"),
			}.String(),
		}, nil

	}

	return nil, nil
}
