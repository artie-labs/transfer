package debezium

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/jsonutil"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/maputil"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

type SupportedDebeziumType string

const (
	JSON    SupportedDebeziumType = "io.debezium.data.Json"
	Enum    SupportedDebeziumType = "io.debezium.data.Enum"
	EnumSet SupportedDebeziumType = "io.debezium.data.EnumSet"
	UUID    SupportedDebeziumType = "io.debezium.data.Uuid"

	Timestamp            SupportedDebeziumType = "io.debezium.time.Timestamp"
	MicroTimestamp       SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
	Date                 SupportedDebeziumType = "io.debezium.time.Date"
	Time                 SupportedDebeziumType = "io.debezium.time.Time"
	MicroTime            SupportedDebeziumType = "io.debezium.time.MicroTime"
	Year                 SupportedDebeziumType = "io.debezium.time.Year"
	TimeWithTimezone     SupportedDebeziumType = "io.debezium.time.ZonedTime"
	DateTimeWithTimezone SupportedDebeziumType = "io.debezium.time.ZonedTimestamp"
	MicroDuration        SupportedDebeziumType = "io.debezium.time.MicroDuration"
	DateKafkaConnect     SupportedDebeziumType = "org.apache.kafka.connect.data.Date"
	TimeKafkaConnect     SupportedDebeziumType = "org.apache.kafka.connect.data.Time"
	DateTimeKafkaConnect SupportedDebeziumType = "org.apache.kafka.connect.data.Timestamp"

	KafkaDecimalType         SupportedDebeziumType = "org.apache.kafka.connect.data.Decimal"
	KafkaVariableNumericType SupportedDebeziumType = "io.debezium.data.VariableScaleDecimal"

	// PostGIS data types
	GeometryPointType SupportedDebeziumType = "io.debezium.data.geometry.Point"
	GeometryType      SupportedDebeziumType = "io.debezium.data.geometry.Geometry"
	GeographyType     SupportedDebeziumType = "io.debezium.data.geometry.Geography"

	KafkaDecimalPrecisionKey = "connect.decimal.precision"
)

// toBytes attempts to convert a value of unknown type to a slice of bytes.
// - If value is already a slice of bytes it will be directly returned.
// - If value is a string we will attempt to base64 decode it.
// - If value is any other type we will convert it to a string and then attempt to base64 decode it.
func toBytes(value any) ([]byte, error) {
	var stringVal string

	switch typedValue := value.(type) {
	case []byte:
		return typedValue, nil
	case string:
		stringVal = typedValue
	default:
		// TODO: Make this a hard error if we don't observe this happening.
		slog.Error("Expected string/[]byte, falling back to fmt.Sprint(value)",
			slog.String("type", fmt.Sprintf("%T", value)),
			slog.Any("value", value),
		)
		stringVal = fmt.Sprint(value)
	}

	data, err := base64.StdEncoding.DecodeString(stringVal)
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode: %w", err)
	}
	return data, nil
}

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

func (f Field) ParseValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	// TODO: Implement a preprocessing step here that reverses the effects of values being JSON marshalled and
	// unmarshalled when they pass through Kafka. This would replace calling f.IsInteger below and might look like:
	// var err error
	// switch f.Type {
	// case Int16, Int32, Int64:
	// 	value, err = toInt64(value)
	// case Bytes:
	// 	value, err = toBytes(value)
	// }
	// if err != nil {
	// 	return nil, err
	// }
	// Once this is in place, the cases in the f.DebeziumType switch statement below won't need to parse int64s or bytes.

	// Check if the field is an integer and requires us to cast it as such.
	if f.IsInteger() {
		value, err := toInt64(value)
		if err != nil {
			return nil, err
		}
		// TODO: Returning an int to preserve existing behavior, however we should see if we can return an int64 instead.
		return int(value), err
	}

	switch f.DebeziumType {
	case JSON:
		if value == constants.ToastUnavailableValuePlaceholder {
			return value, nil
		}
		return jsonutil.SanitizePayload(value)
	case GeometryType, GeographyType:
		return parseGeometry(value)
	case GeometryPointType:
		return parseGeometryPoint(value)
	case KafkaDecimalType:
		bytes, err := toBytes(value)
		if err != nil {
			return nil, err
		}
		return f.DecodeDecimal(bytes)
	case KafkaVariableNumericType:
		return f.DecodeDebeziumVariableDecimal(value)
	case
		Timestamp,
		MicroTimestamp,
		Date,
		Time,
		MicroTime,
		DateKafkaConnect,
		TimeKafkaConnect,
		DateTimeKafkaConnect:

		switch value.(type) {
		case float64:
			// This value is coming from Kafka, and will have been marshaled to a JSON string, so when it is
			// unmarshalled it will be a float64
			// -> Pass.
		case int64:
			// This value is coming from reader.
			// -> Pass.
		default:
			// Value should always be a float64/int64, but let's check this if this assumption holds and if so clean up
			// the code below so that we aren't doing float -> string -> float.
			slog.Error(fmt.Sprintf("Expected float64 received %T with value '%v'", value, value))
		}

		switch f.Type {
		case Int16, Int32, Int64:
			// These are the types we expect.
			// -> Pass
		default:
			// Assuming we always receive an int type for Debeziumn time types, we should be able to add a preprocessing
			// step above that inspects the field type and if it is an int type casts `value` to `int64`.
			slog.Error(fmt.Sprintf("Unexpected field type '%s' for Debezium time type '%s'", f.Type, f.DebeziumType),
				slog.String("type", fmt.Sprintf("%T", value)),
				slog.Any("value", value),
			)
		}

		// Need to cast this as a FLOAT first because the number may come out in scientific notation
		// ParseFloat is apt to handle it, and ParseInt is not, see: https://github.com/golang/go/issues/19288
		floatVal, castErr := strconv.ParseFloat(fmt.Sprint(value), 64)
		if castErr != nil {
			return nil, castErr
		}
		int64Val := int64(floatVal)

		int64ValFromFunc, err := toInt64(value)
		if err != nil {
			slog.Error("Unable to call toInt64", slog.Any("err", err))
		}

		if int64Val != int64ValFromFunc {
			slog.Error("int64Val is different from int64ValFromFunc",
				slog.Int64("int64Val", int64Val),
				slog.Int64("int64ValFromFunc", int64ValFromFunc),
				slog.Any("value", value),
			)
		}

		return FromDebeziumTypeToTime(f.DebeziumType, int64Val)
	}

	if bytes, ok := value.([]byte); ok {
		// Preserve existing behavior by base64 encoding []byte values to a string.
		// TODO: Look into inverting this logic so that in the case the field type is "bytes" but the value is a string we
		// base64 decode it in a preprocessing step above. Then things downstream from this method can just deal with []byte values.
		return base64.StdEncoding.EncodeToString(bytes), nil
	}

	return value, nil
}

// FromDebeziumTypeToTime is implemented by following this spec: https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-temporal-types
func FromDebeziumTypeToTime(supportedType SupportedDebeziumType, val int64) (*ext.ExtendedTime, error) {
	var extTime *ext.ExtendedTime

	switch supportedType {
	case Timestamp, DateTimeKafkaConnect:
		// Represents the number of milliseconds since the epoch, and does not include timezone information.
		extTime = ext.NewExtendedTime(time.UnixMilli(val).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case MicroTimestamp:
		// Represents the number of microseconds since the epoch, and does not include timezone information.
		extTime = ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case Date, DateKafkaConnect:
		unix := time.UnixMilli(0).In(time.UTC) // 1970-01-01
		// Represents the number of days since the epoch.
		extTime = ext.NewExtendedTime(unix.AddDate(0, 0, int(val)), ext.DateKindType, "")
	case Time, TimeKafkaConnect:
		// Represents the number of milliseconds past midnight, and does not include timezone information.
		extTime = ext.NewExtendedTime(time.UnixMilli(val).In(time.UTC), ext.TimeKindType, "")
	case MicroTime:
		// Represents the number of microseconds past midnight, and does not include timezone information.
		extTime = ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.TimeKindType, "")
	default:
		return nil, fmt.Errorf("supportedType: %s, val: %v failed to be matched", supportedType, val)
	}

	if extTime != nil && !extTime.IsValid() {
		return nil, fmt.Errorf("extTime is invalid: %v", extTime)
	}

	return extTime, nil
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
	return DecodeDecimal(encoded, precision, scale), nil
}

func (f Field) DecodeDebeziumVariableDecimal(value any) (*decimal.Decimal, error) {
	valueStruct, isOk := value.(map[string]any)
	if !isOk {
		return nil, fmt.Errorf("value is not map[string]any type")
	}

	scale, err := maputil.GetIntegerFromMap(valueStruct, "scale")
	if err != nil {
		return nil, err
	}

	val, isOk := valueStruct["value"]
	if !isOk {
		return nil, fmt.Errorf("encoded value does not exist")
	}

	bytes, err := toBytes(val)
	if err != nil {
		return nil, err
	}
	return DecodeDecimal(bytes, ptr.ToInt(decimal.PrecisionNotSpecified), scale), nil
}
