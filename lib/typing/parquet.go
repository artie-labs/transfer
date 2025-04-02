package typing

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/xitongsys/parquet-go/parquet"
)

type FieldTag struct {
	Name               string
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
	case String.Kind, Struct.Kind, Date.Kind, Time.Kind:
		return &Field{
			Tag: FieldTag{
				Name:          colName,
				Type:          ToPtr("BYTE_ARRAY"),
				ConvertedType: ToPtr("UTF8"),
			}.String(),
		}, nil
	case Float.Kind:
		return &Field{
			Tag: FieldTag{
				Name: colName,
				Type: ToPtr("FLOAT"),
			}.String(),
		}, nil
	case Integer.Kind, TimestampNTZ.Kind, TimestampTZ.Kind:
		return &Field{
			Tag: FieldTag{
				Name: colName,
				Type: ToPtr("INT64"),
			}.String(),
		}, nil
	case EDecimal.Kind:
		precision := k.ExtendedDecimalDetails.Precision()
		if precision == decimal.PrecisionNotSpecified {
			fmt.Println("colName", colName, "precision was not specified")
			// Precision is required for a parquet DECIMAL type, as such, we should fall back on a STRING type.
			return &Field{
				Tag: FieldTag{
					Name:          colName,
					Type:          ToPtr("BYTE_ARRAY"),
					ConvertedType: ToPtr("UTF8"),
				}.String(),
			}, nil
		}

		scale := k.ExtendedDecimalDetails.Scale()
		if scale > precision {
			return nil, fmt.Errorf("scale (%d) must be less than or equal to precision (%d)", scale, precision)
		}

		fmt.Println("colName", colName, "byte array", "decimal", "precision", precision, "scale", scale)
		return &Field{
			Tag: FieldTag{
				Name:          colName,
				Type:          ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY.String()),
				ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL.String()),
				Precision:     ToPtr(int(precision)),
				Scale:         ToPtr(int(scale)),
				Length:        ToPtr(int(precision + scale)),
			}.String(),
		}, nil
	case Boolean.Kind:
		return &Field{
			Tag: FieldTag{
				Name: colName,
				Type: ToPtr("BOOLEAN"),
			}.String(),
		}, nil
	case Array.Kind:
		return &Field{
			Tag: FieldTag{
				Name:           colName,
				Type:           ToPtr("LIST"),
				RepetitionType: ToPtr("REQUIRED"),
			}.String(),
			Fields: []Field{
				{
					Tag: FieldTag{
						Name:           "element",
						Type:           ToPtr("BYTE_ARRAY"),
						ConvertedType:  ToPtr("UTF8"),
						RepetitionType: ToPtr("REQUIRED"),
					}.String(),
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported kind: %q", k.Kind)
	}
}
