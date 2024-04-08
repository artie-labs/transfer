package debezium

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"time"

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

func (f Field) ParseValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	// Check if the field is an integer and requires us to cast it as such.
	if f.IsInteger() {
		valFloat, isOk := value.(float64)
		if !isOk {
			return nil, fmt.Errorf("failed to cast value to float64")
		}

		return int(valFloat), nil
	}

	switch f.DebeziumType {
	case JSON:
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
		if _, ok := value.(float64); !ok {
			// Since this value is coming from Kafka, and will have been marshaled to a JSON string, it should always
			// be a float64. Let's check this if this assumption holds and if so clean up the code below so that we
			// aren't doing float -> string -> float.
			slog.Error(fmt.Sprintf("Expected float64 received %T with value '%v'", value, value))
		}
		// Need to cast this as a FLOAT first because the number may come out in scientific notation
		// ParseFloat is apt to handle it, and ParseInt is not, see: https://github.com/golang/go/issues/19288
		floatVal, castErr := strconv.ParseFloat(fmt.Sprint(value), 64)
		if castErr != nil {
			return nil, castErr
		}

		return FromDebeziumTypeToTime(f.DebeziumType, int64(floatVal))
	}

	return value, nil
}

// FromDebeziumTypeToTime is implemented by following this spec: https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-temporal-types
func FromDebeziumTypeToTime(supportedType SupportedDebeziumType, val int64) (*ext.ExtendedTime, error) {
	var extTime *ext.ExtendedTime
	var err error

	switch supportedType {
	case Timestamp, DateTimeKafkaConnect:
		// Represents the number of milliseconds since the epoch, and does not include timezone information.
		extTime, err = ext.NewExtendedTime(time.UnixMilli(val).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case MicroTimestamp:
		// Represents the number of microseconds since the epoch, and does not include timezone information.
		extTime, err = ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case Date, DateKafkaConnect:
		unix := time.UnixMilli(0).In(time.UTC) // 1970-01-01
		// Represents the number of days since the epoch.
		extTime, err = ext.NewExtendedTime(unix.AddDate(0, 0, int(val)), ext.DateKindType, "")
	case Time, TimeKafkaConnect:
		// Represents the number of milliseconds past midnight, and does not include timezone information.
		extTime, err = ext.NewExtendedTime(time.UnixMilli(val).In(time.UTC), ext.TimeKindType, "")
	case MicroTime:
		// Represents the number of microseconds past midnight, and does not include timezone information.
		extTime, err = ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.TimeKindType, "")
	default:
		return nil, fmt.Errorf("supportedType: %s, val: %v failed to be matched", supportedType, val)
	}

	if err != nil {
		return nil, err
	}

	if extTime != nil && !extTime.IsValid() {
		return nil, fmt.Errorf("extTime is invalid: %v", extTime)
	}

	return extTime, err
}

// DecodeDecimal is used to handle `org.apache.kafka.connect.data.Decimal` where this would be emitted by Debezium when the `decimal.handling.mode` is `precise`
// * Encoded - takes the encoded value as a slice of bytes
// * Parameters - which contains:
//   - `scale` (number of digits following decimal point)
//   - `connect.decimal.precision` which is an optional parameter. (If -1, then it's variable and .Value() will be in STRING).
func (f Field) DecodeDecimal(encoded []byte) (*decimal.Decimal, error) {
	results, err := f.GetScaleAndPrecision()
	if err != nil {
		return nil, fmt.Errorf("failed to get scale and/or precision: %w", err)
	}
	return DecodeDecimal(encoded, results.Precision, results.Scale), nil
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
