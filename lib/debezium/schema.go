package debezium

import (
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
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
	Int8    FieldType = "int8"
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
	// [ItemsMetadata] is only populated if the literal type is an array.
	ItemsMetadata *Field `json:"items,omitempty"`
}

func (f Field) GetScaleAndPrecision() (int32, *int32, error) {
	scale, scaleErr := maputil.GetInt32FromMap(f.Parameters, "scale")
	if scaleErr != nil {
		return 0, nil, scaleErr
	}

	var precisionPtr *int32
	if _, ok := f.Parameters[KafkaDecimalPrecisionKey]; ok {
		precision, precisionErr := maputil.GetInt32FromMap(f.Parameters, KafkaDecimalPrecisionKey)
		if precisionErr != nil {
			return 0, nil, precisionErr
		}

		precisionPtr = typing.ToPtr(int32(precision))
	}

	return scale, precisionPtr, nil
}

func (f Field) ToValueConverter() (converters.ValueConverter, error) {
	switch f.DebeziumType {
	// Passthrough converters
	case UUID, LTree, Enum, EnumSet, Interval, XML:
		return converters.StringPassthrough{}, nil
	case Year, MicroDuration:
		return &converters.Int64Passthrough{}, nil
	case Bits:
		return converters.Base64{}, nil
	case ZonedTimestamp:
		return converters.ZonedTimestamp{}, nil
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
	// Decimal
	case KafkaVariableNumericType:
		return converters.NewVariableDecimal(), nil
	case KafkaDecimalType:
		scale, precisionPtr, err := f.GetScaleAndPrecision()
		if err != nil {
			return nil, err
		}

		precision := decimal.PrecisionNotSpecified
		if precisionPtr != nil {
			precision = *precisionPtr
		}

		return converters.NewDecimal(decimal.NewDetails(precision, scale)), nil
	// Time
	case Time, TimeKafkaConnect:
		return converters.Time{}, nil
	case NanoTime:
		return converters.NanoTime{}, nil
	case MicroTime:
		return converters.MicroTime{}, nil
	// Timestamp
	case Timestamp, TimestampKafkaConnect:
		return converters.Timestamp{}, nil
	case MicroTimestamp:
		return converters.MicroTimestamp{}, nil
	case NanoTimestamp:
		return converters.NanoTimestamp{}, nil
	default:
		if f.DebeziumType != "" {
			slog.Warn("Unhandled Debezium type", slog.String("type", string(f.Type)), slog.String("debeziumType", string(f.DebeziumType)))
		}

		switch f.Type {
		case Array:
			if f.ItemsMetadata == nil {
				// TODO: Remove this condition once Reader fully supports setting items metadata
				return converters.NewArray(nil), nil
			}
			return converters.NewArray(f.ItemsMetadata.ParseValue), nil
		case Double, Float:
			return converters.Float64{}, nil
		}

		return nil, nil
	}
}

func (f Field) ToKindDetails(cfg config.SharedDestinationSettings) (typing.KindDetails, error) {
	if cfg.ForceUTCTimezone {
		switch f.DebeziumType {
		case Timestamp, TimestampKafkaConnect, MicroTimestamp, NanoTimestamp:
			return typing.TimestampTZ, nil
		}
	}

	// Prioritize converters
	converter, err := f.ToValueConverter()
	if err != nil {
		return typing.Invalid, err
	}

	if converter != nil {
		return converter.ToKindDetails(), nil
	}

	switch f.Type {
	case Map:
		return typing.Struct, nil
	case Int8, Int16, Int32, Int64:
		return typing.Integer, nil
	case String:
		return typing.String, nil
	case Bytes:
		if cfg.ColumnSettings.WriteRawBinaryValues {
			return typing.Bytes, nil
		}
		return typing.String, nil
	case Struct:
		return typing.Struct, nil
	case Boolean:
		return typing.Boolean, nil
	default:
		return typing.Invalid, fmt.Errorf("unhandled field type %q", f.Type)
	}
}
