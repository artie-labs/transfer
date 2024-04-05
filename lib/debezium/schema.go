package debezium

import (
	"github.com/artie-labs/transfer/lib/cdc"
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

func (s *Schema) GetSchemaFromLabel(kind cdc.FieldLabelKind) *FieldsObject {
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
	Optional   bool               `json:"optional"`
	FieldLabel cdc.FieldLabelKind `json:"field"`
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

func (f Field) IsInteger() (valid bool) {
	return f.ToKindDetails() == typing.Integer
}

type ScaleAndPrecisionResults struct {
	Scale     int
	Precision *int
}

func (f Field) GetScaleAndPrecision() (ScaleAndPrecisionResults, error) {
	scale, scaleErr := maputil.GetIntegerFromMap(f.Parameters, "scale")
	if scaleErr != nil {
		return ScaleAndPrecisionResults{}, scaleErr
	}

	var precisionPtr *int
	if _, isOk := f.Parameters[KafkaDecimalPrecisionKey]; isOk {
		precision, precisionErr := maputil.GetIntegerFromMap(f.Parameters, KafkaDecimalPrecisionKey)
		if precisionErr != nil {
			return ScaleAndPrecisionResults{}, precisionErr
		}

		precisionPtr = ptr.ToInt(precision)
	}

	return ScaleAndPrecisionResults{
		Scale:     scale,
		Precision: precisionPtr,
	}, nil
}

func (f Field) ToKindDetails() typing.KindDetails {
	// We'll first cast based on Debezium types
	// Then, we'll fall back on the actual data types.
	switch f.DebeziumType {
	case Timestamp, MicroTimestamp, DateTimeKafkaConnect, DateTimeWithTimezone:
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)
	case Date, DateKafkaConnect:
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType)
	case Time, TimeMicro, TimeKafkaConnect, TimeWithTimezone:
		return typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)
	case JSON, GeometryPointType, GeometryType, GeographyType:
		return typing.Struct
	case KafkaDecimalType:
		scaleAndPrecision, err := f.GetScaleAndPrecision()
		if err != nil {
			return typing.Invalid
		}

		eDecimal := typing.EDecimal
		eDecimal.ExtendedDecimalDetails = decimal.NewDecimal(scaleAndPrecision.Precision, scaleAndPrecision.Scale, nil)
		return eDecimal
	case KafkaVariableNumericType:
		// For variable numeric types, we are defaulting to a scale of 5
		// This is because scale is not specified at the column level, rather at the row level
		// It shouldn't matter much anyway since the column type we are creating is `TEXT` to avoid boundary errors.
		eDecimal := typing.EDecimal
		eDecimal.ExtendedDecimalDetails = decimal.NewDecimal(ptr.ToInt(decimal.PrecisionNotSpecified), decimal.DefaultScale, nil)
		return eDecimal
	}

	switch f.Type {
	case Map:
		return typing.Struct
	case Int16, Int32, Int64:
		return typing.Integer
	case Float, Double:
		return typing.Float
	case String, Bytes:
		return typing.String
	case Struct:
		return typing.Struct
	case Boolean:
		return typing.Boolean
	case Array:
		return typing.Array
	default:
		return typing.Invalid
	}
}
