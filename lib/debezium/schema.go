package debezium

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Schema struct {
	SchemaType   string         `json:"type"`
	FieldsObject []FieldsObject `json:"fields"`
}

func (s *Schema) GetSchemaFromLabel(kind FieldLabelKind) *FieldsObject {
	for _, fieldObject := range s.FieldsObject {
		if fieldObject.FieldLabel == kind {
			return &fieldObject
		}
	}

	return nil
}

type FieldsObject struct {
	// What the type is for the block of field, e.g. STRUCT, or STRING.
	FieldObjectType string `json:"type"`

	// The actual schema object.
	Fields []Field `json:"fields"`

	// Whether this block for "after", "before", exists
	Optional   bool           `json:"optional"`
	FieldLabel FieldLabelKind `json:"field"`
}

type FieldType string

const (
	String  FieldType = "string"
	Bytes   FieldType = "bytes"
	Boolean FieldType = "boolean"
	Int16   FieldType = "int16"
	Int32   FieldType = "int32"
	Int64   FieldType = "int64"
	Float   FieldType = "float"
	Double  FieldType = "double"
	Struct  FieldType = "struct"
	Array   FieldType = "array"
	Map     FieldType = "map"
)

type Field struct {
	Type         FieldType             `json:"type"`
	Optional     bool                  `json:"optional"`
	Default      any                   `json:"default"`
	FieldName    string                `json:"field"`
	DebeziumType SupportedDebeziumType `json:"name"`
	Parameters   map[string]any        `json:"parameters"`
}

func (f Field) GetScaleAndPrecision() (int32, *int32, error) {
	scale, scaleErr := maputil.GetInt32FromMap(f.Parameters, "scale")
	if scaleErr != nil {
		return 0, nil, scaleErr
	}

	var precisionPtr *int32
	if _, isOk := f.Parameters[KafkaDecimalPrecisionKey]; isOk {
		precision, precisionErr := maputil.GetInt32FromMap(f.Parameters, KafkaDecimalPrecisionKey)
		if precisionErr != nil {
			return 0, nil, precisionErr
		}

		precisionPtr = ptr.ToInt32(precision)
	}

	return scale, precisionPtr, nil
}

func (f Field) ToValueConverter() (converters.ValueConverter, error) {
	switch f.DebeziumType {
	case DateTimeWithTimezone:
		return converters.DateTimeWithTimezone{}, nil
	case TimeWithTimezone:
		return converters.TimeWithTimezone{}, nil
	case GeometryPointType:
		return converters.GeometryPoint{}, nil
	case GeographyType, GeometryType:
		return converters.Geometry{}, nil
	case JSON:
		return converters.JSON{}, nil
	case Date, DateKafkaConnect:
		return converters.Date{}, nil
	case Time, TimeKafkaConnect:
		return converters.Time{}, nil
	case KafkaDecimalType:
		scale, precisionPtr, err := f.GetScaleAndPrecision()
		if err != nil {
			return nil, fmt.Errorf("failed to get scale and precision: %w", err)
		}

		precision := decimal.PrecisionNotSpecified
		if precisionPtr != nil {
			precision = *precisionPtr
		}

		return converters.NewDecimal(precision, scale, false), nil
	case KafkaVariableNumericType:
		// For variable numeric types, we are defaulting to a scale of 5
		// This is because scale is not specified at the column level, rather at the row level
		// It shouldn't matter much anyway since the column type we are creating is `TEXT` to avoid boundary errors.
		return converters.NewDecimal(decimal.PrecisionNotSpecified, decimal.DefaultScale, true), nil
	}

	return nil, nil
}

func (f Field) ToKindDetails() (typing.KindDetails, error) {
	// Prioritize converters
	converter, err := f.ToValueConverter()
	if err != nil {
		return typing.Invalid, err
	}

	if converter != nil {
		return converter.ToKindDetails(), nil
	}

	// TODO: Deprecate this in favor of the converters

	// We'll first cast based on Debezium types
	// Then, we'll fall back on the actual data types.
	switch f.DebeziumType {
	case Timestamp, MicroTimestamp, NanoTimestamp, DateTimeKafkaConnect:
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType), nil
	case MicroTime, NanoTime:
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType), nil
	}

	switch f.Type {
	case Map:
		return typing.Struct, nil
	case Int16, Int32, Int64:
		return typing.Integer, nil
	case Float, Double:
		return typing.Float, nil
	case String, Bytes:
		return typing.String, nil
	case Struct:
		return typing.Struct, nil
	case Boolean:
		return typing.Boolean, nil
	case Array:
		return typing.Array, nil
	default:
		// TODO: Throw an error
		return typing.Invalid, nil
	}
}
