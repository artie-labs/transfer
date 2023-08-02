package typing

import "github.com/artie-labs/transfer/lib/ptr"

type FieldTag struct {
	Name               string
	InName             *string
	Type               string
	ConvertedType      *string
	ValueConvertedType *string
	// https://github.com/xitongsys/parquet-go#repetition-type
	RepetitionType *string
}

type Field struct {
	Tag    FieldTag `json:"Tag"`
	Fields []Field  `json:"Fields"`
}

func (k *KindDetails) ParquetAnnotation(colName string) (*Field, error) {
	switch k.Kind {
	case Float.Kind:
		return &Field{
			Tag: FieldTag{
				Name:   colName,
				InName: &colName,
				Type:   "FLOAT",
			},
		}, nil
	case Integer.Kind, ETime.Kind:
		// Parquet doesn't have native time types, so we are using int64 and casting the value as UNIX ts.
		return &Field{
			Tag: FieldTag{
				Name:   colName,
				InName: &colName,
				Type:   "INT64",
			},
		}, nil
	case EDecimal.Kind:
		// TODO: Support precision and scale.
		return &Field{
			Tag: FieldTag{
				Name:   colName,
				InName: &colName,
				Type:   "INT64",
			},
		}, nil
	case Boolean.Kind:
		return &Field{
			Tag: FieldTag{
				Name:   colName,
				InName: &colName,
				Type:   "BOOLEAN",
			},
		}, nil
	case Array.Kind:
		return &Field{
			Tag: FieldTag{
				Name:           colName,
				InName:         &colName,
				Type:           "LIST",
				RepetitionType: ptr.ToString("REQUIRED"),
			},
			Fields: []Field{
				{
					Tag: FieldTag{
						Name:           "element",
						Type:           "BYTE_ARRAY",
						ConvertedType:  ptr.ToString("UTF8"),
						RepetitionType: ptr.ToString("REQUIRED"),
					},
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
				Type:          "BYTE_ARRAY",
				ConvertedType: ptr.ToString("UTF8"),
			},
		}, nil

	}

	return nil, nil
}
