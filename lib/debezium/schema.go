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

type Field struct {
	Type         string                 `json:"type"`
	Optional     bool                   `json:"optional"`
	Default      interface{}            `json:"default"`
	FieldName    string                 `json:"field"`
	DebeziumType string                 `json:"name"`
	Parameters   map[string]interface{} `json:"parameters"`
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
	case string(Timestamp), string(MicroTimestamp), string(DateTimeKafkaConnect), string(DateTimeWithTimezone):
		etime := typing.ETime
		etime.ExtendedTimeDetails = &ext.NestedKind{
			Type: ext.DateTimeKindType,
		}
		return etime
	case string(Date), string(DateKafkaConnect):
		etime := typing.ETime
		etime.ExtendedTimeDetails = &ext.NestedKind{
			Type: ext.DateKindType,
		}
		return etime
	case string(Time), string(TimeMicro), string(TimeKafkaConnect), string(TimeWithTimezone):
		etime := typing.ETime
		etime.ExtendedTimeDetails = &ext.NestedKind{
			Type: ext.TimeKindType,
		}
		return etime
	case string(JSON):
		return typing.Struct
	case string(KafkaDecimalType):
		scaleAndPrecision, err := f.GetScaleAndPrecision()
		if err != nil {
			return typing.Invalid
		}

		eDecimal := typing.EDecimal
		eDecimal.ExtendedDecimalDetails = decimal.NewDecimal(scaleAndPrecision.Scale, scaleAndPrecision.Precision, nil)
		return eDecimal
	case string(KafkaVariableNumericType):
		// For variable numeric types, we are defaulting to a scale of 5
		// This is because scale is not specified at the column level, rather at the row level
		// It shouldn't matter much anyway since the column type we are creating is `TEXT` to avoid boundary errors.
		eDecimal := typing.EDecimal
		eDecimal.ExtendedDecimalDetails = decimal.NewDecimal(decimal.DefaultScale, ptr.ToInt(decimal.PrecisionNotSpecified), nil)
		return eDecimal
	}

	switch f.Type {
	case "int16", "int32", "int64":
		return typing.Integer
	case "float", "double":
		return typing.Float
	case "string":
		return typing.String
	case "struct":
		return typing.Struct
	case "boolean":
		return typing.Boolean
	case "array":
		return typing.Array
	default:
		return typing.Invalid
	}
}
