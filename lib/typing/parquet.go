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
	// This is used for timestamps only:
	IsAdjustedForUTC *bool
	Unit             *string
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

	if f.IsAdjustedForUTC != nil {
		parts = append(parts, fmt.Sprintf("isAdjustedToUTC=%t", *f.IsAdjustedForUTC))
	}

	if f.Unit != nil {
		parts = append(parts, fmt.Sprintf("unit=%s", *f.Unit))
	}

	return strings.Join(parts, ", ")
}

type Field struct {
	Tag    string  `json:"Tag"`
	Fields []Field `json:"Fields,omitempty"`
}

func (k *KindDetails) ParquetAnnotation(colName string) (*Field, error) {
	switch k.Kind {
	case String.Kind, Struct.Kind:
		return &Field{
			Tag: FieldTag{
				Name:          colName,
				Type:          ToPtr(parquet.Type_BYTE_ARRAY.String()),
				ConvertedType: ToPtr(parquet.ConvertedType_UTF8.String()),
			}.String(),
		}, nil
	case Float.Kind:
		return &Field{
			Tag: FieldTag{
				Name: colName,
				Type: ToPtr(parquet.Type_FLOAT.String()),
			}.String(),
		}, nil
	case Date.Kind:
		return &Field{
			Tag: FieldTag{
				Name:          colName,
				Type:          ToPtr(parquet.Type_INT32.String()),
				ConvertedType: ToPtr(parquet.ConvertedType_DATE.String()),
			}.String(),
		}, nil
	case Time.Kind:
		return &Field{
			Tag: FieldTag{
				Name:          colName,
				Type:          ToPtr(parquet.Type_INT32.String()),
				ConvertedType: ToPtr(parquet.ConvertedType_TIME_MILLIS.String()),
			}.String(),
		}, nil
	case TimestampNTZ.Kind, TimestampTZ.Kind:
		return &Field{
			Tag: FieldTag{
				Name:             colName,
				Type:             ToPtr(parquet.Type_INT64.String()),
				IsAdjustedForUTC: ToPtr(true),
				Unit:             ToPtr("MILLIS"),
			}.String(),
		}, nil
	case Integer.Kind:
		return &Field{
			Tag: FieldTag{
				Name: colName,
				Type: ToPtr(parquet.Type_INT64.String()),
			}.String(),
		}, nil
	case EDecimal.Kind:
		precision := k.ExtendedDecimalDetails.Precision()
		if precision == decimal.PrecisionNotSpecified {
			// Precision is required for a parquet DECIMAL type, as such, we should fall back on a STRING type.
			return &Field{
				Tag: FieldTag{
					Name:          colName,
					Type:          ToPtr(parquet.Type_BYTE_ARRAY.String()),
					ConvertedType: ToPtr(parquet.ConvertedType_UTF8.String()),
				}.String(),
			}, nil
		}

		scale := k.ExtendedDecimalDetails.Scale()
		if scale > precision {
			return nil, fmt.Errorf("scale (%d) must be less than or equal to precision (%d)", scale, precision)
		}

		return &Field{
			Tag: FieldTag{
				Name:          colName,
				Type:          ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY.String()),
				ConvertedType: ToPtr(parquet.ConvertedType_DECIMAL.String()),
				Precision:     ToPtr(int(precision)),
				Scale:         ToPtr(int(scale)),
				Length:        ToPtr(int(k.ExtendedDecimalDetails.TwosComplementByteArrLength())),
			}.String(),
		}, nil
	case Boolean.Kind:
		return &Field{
			Tag: FieldTag{
				Name: colName,
				Type: ToPtr(parquet.Type_BOOLEAN.String()),
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
						Type:           ToPtr(parquet.Type_BYTE_ARRAY.String()),
						ConvertedType:  ToPtr(parquet.ConvertedType_UTF8.String()),
						RepetitionType: ToPtr("REQUIRED"),
					}.String(),
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported kind: %q", k.Kind)
	}
}
