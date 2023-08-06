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
	Scale          *int
	Precision      *int
	Length         *int
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

	if f.Scale != nil {
		parts = append(parts, fmt.Sprintf("scale=%v", *f.Scale))
	}

	if f.Precision != nil {
		parts = append(parts, fmt.Sprintf("precision=%v", *f.Precision))
	}

	if f.Length != nil {
		parts = append(parts, fmt.Sprintf("length=%v", *f.Length))
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
		precision := k.ExtendedDecimalDetails.Precision()
		if precision == nil || *precision == -1 {
			// This is a variable precision decimal, so we'll just treat it as a string.
			return &Field{
				Tag: FieldTag{
					Name:          colName,
					InName:        &colName,
					Type:          ptr.ToString("BYTE_ARRAY"),
					ConvertedType: ptr.ToString("UTF8"),
				}.String(),
			}, nil
		}

		scale := k.ExtendedDecimalDetails.Scale()
		return &Field{
			Tag: FieldTag{
				Name:          colName,
				InName:        &colName,
				Type:          ptr.ToString("BYTE_ARRAY"),
				ConvertedType: ptr.ToString("DECIMAL"),
				Precision:     precision,
				Scale:         ptr.ToInt(scale),
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
