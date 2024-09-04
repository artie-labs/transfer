package debezium

import (
	"encoding/base64"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"github.com/artie-labs/transfer/lib/debezium/converters"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

// FieldLabelKind is used when the schema is turned on. Each schema object will be labelled.
type FieldLabelKind string

const (
	Before      FieldLabelKind = "before"
	After       FieldLabelKind = "after"
	Source      FieldLabelKind = "source"
	Op          FieldLabelKind = "op"
	TsMs        FieldLabelKind = "ts_ms"
	Transaction FieldLabelKind = "transaction"
)

type SupportedDebeziumType string

const (
	JSON    SupportedDebeziumType = "io.debezium.data.Json"
	Enum    SupportedDebeziumType = "io.debezium.data.Enum"
	EnumSet SupportedDebeziumType = "io.debezium.data.EnumSet"
	UUID    SupportedDebeziumType = "io.debezium.data.Uuid"
	LTree   SupportedDebeziumType = "io.debezium.data.Ltree"

	// Dates
	Date                 SupportedDebeziumType = "io.debezium.time.Date"
	DateKafkaConnect     SupportedDebeziumType = "org.apache.kafka.connect.data.Date"
	DateTimeWithTimezone SupportedDebeziumType = "io.debezium.time.ZonedTimestamp"
	MicroDuration        SupportedDebeziumType = "io.debezium.time.MicroDuration"
	Year                 SupportedDebeziumType = "io.debezium.time.Year"

	// Time
	Time             SupportedDebeziumType = "io.debezium.time.Time"
	MicroTime        SupportedDebeziumType = "io.debezium.time.MicroTime"
	NanoTime         SupportedDebeziumType = "io.debezium.time.NanoTime"
	TimeWithTimezone SupportedDebeziumType = "io.debezium.time.ZonedTime"
	TimeKafkaConnect SupportedDebeziumType = "org.apache.kafka.connect.data.Time"

	// Timestamps
	MicroTimestamp        SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
	NanoTimestamp         SupportedDebeziumType = "io.debezium.time.NanoTimestamp"
	Timestamp             SupportedDebeziumType = "io.debezium.time.Timestamp"
	TimestampKafkaConnect SupportedDebeziumType = "org.apache.kafka.connect.data.Timestamp"

	KafkaDecimalType         SupportedDebeziumType = "org.apache.kafka.connect.data.Decimal"
	KafkaVariableNumericType SupportedDebeziumType = "io.debezium.data.VariableScaleDecimal"

	// PostGIS data types
	GeometryPointType SupportedDebeziumType = "io.debezium.data.geometry.Point"
	GeometryType      SupportedDebeziumType = "io.debezium.data.geometry.Geometry"
	GeographyType     SupportedDebeziumType = "io.debezium.data.geometry.Geography"

	KafkaDecimalPrecisionKey = "connect.decimal.precision"
)

// toInt64 attempts to convert a value of unknown type to a an int64.
// - If the value is coming from Kafka it will be decoded as a float64 when it is unmarshalled from JSON.
// - If the value is coming from reader the value will be an int16/int32/int64.
func toInt64(value any) (int64, error) {
	switch typedValue := value.(type) {
	case int:
		return int64(typedValue), nil
	case int16:
		return int64(typedValue), nil
	case int32:
		return int64(typedValue), nil
	case int64:
		return typedValue, nil
	case float64:
		return int64(typedValue), nil
	}
	return 0, fmt.Errorf("failed to cast value '%v' with type '%T' to int64", value, value)
}

// ShouldSetDefaultValue will filter out computed fields that cannot be properly set with a default value
func (f Field) ShouldSetDefaultValue(defaultValue any) bool {
	switch castedDefaultValue := defaultValue.(type) {
	case nil:
		return false
	case *ext.ExtendedTime:
		return !castedDefaultValue.Time.IsZero()
	case string:
		if f.DebeziumType == UUID && castedDefaultValue == uuid.Nil.String() {
			return false
		}

		return true
	case bool, int, int16, int32, int64, float32, float64, *decimal.Decimal:
		return true
	default:
		// TODO: Remove this after some time.
		slog.Warn("Default value that we did not add a case for yet, we're returning true",
			slog.String("type", fmt.Sprintf("%T", defaultValue)),
			slog.Any("defaultValue", defaultValue),
		)
	}

	return true
}

func (f Field) ParseValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	// Preprocess [value] to reverse the effects of being JSON marshalled and unmarshalled when passing through Kafka.
	switch f.Type {
	case Int16, Int32, Int64:
		var err error
		value, err = toInt64(value)
		if err != nil {
			return nil, err
		}
	case Bytes:
		var err error
		value, err = converters.Bytes{}.Convert(value)
		if err != nil {
			return nil, err
		}
	}

	if converter := f.ToValueConverter(); converter != nil {
		return converter.Convert(value)
	}

	switch f.DebeziumType {
	case KafkaDecimalType:
		castedBytes, err := typing.AssertType[[]byte](value)
		if err != nil {
			return nil, err
		}

		return f.DecodeDecimal(castedBytes)
	case KafkaVariableNumericType:
		return f.DecodeDebeziumVariableDecimal(value)
	}

	if bytes, ok := value.([]byte); ok {
		// Preserve existing behavior by base64 encoding []byte values to a string.
		// TODO: Look into inverting this logic so that in the case the field type is "bytes" but the value is a string we
		// base64 decode it in a preprocessing step above. Then things downstream from this method can just deal with []byte values.
		return base64.StdEncoding.EncodeToString(bytes), nil
	}

	return value, nil
}

// DecodeDecimal is used to handle `org.apache.kafka.connect.data.Decimal` where this would be emitted by Debezium when the `decimal.handling.mode` is `precise`
// * Encoded - takes the encoded value as a slice of bytes
// * Parameters - which contains:
//   - `scale` (number of digits following decimal point)
//   - `connect.decimal.precision` which is an optional parameter. (If -1, then it's variable and .Value() will be in STRING).
func (f Field) DecodeDecimal(encoded []byte) (*decimal.Decimal, error) {
	scale, precision, err := f.GetScaleAndPrecision()
	if err != nil {
		return nil, fmt.Errorf("failed to get scale and/or precision: %w", err)
	}

	_decimal := converters.DecodeDecimal(encoded, scale)
	if precision == nil {
		return decimal.NewDecimal(_decimal), nil
	}

	return decimal.NewDecimalWithPrecision(_decimal, *precision), nil
}

func (Field) DecodeDebeziumVariableDecimal(value any) (*decimal.Decimal, error) {
	valueStruct, isOk := value.(map[string]any)
	if !isOk {
		return nil, fmt.Errorf("value is not map[string]any type")
	}

	scale, err := maputil.GetInt32FromMap(valueStruct, "scale")
	if err != nil {
		return nil, err
	}

	val, isOk := valueStruct["value"]
	if !isOk {
		return nil, fmt.Errorf("encoded value does not exist")
	}

	bytes, err := converters.Bytes{}.Convert(val)
	if err != nil {
		return nil, err
	}

	castedBytes, err := typing.AssertType[[]byte](bytes)
	if err != nil {
		return nil, err
	}

	return decimal.NewDecimal(converters.DecodeDecimal(castedBytes, scale)), nil
}
