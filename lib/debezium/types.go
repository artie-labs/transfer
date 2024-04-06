package debezium

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/maputil"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

type SupportedDebeziumType string

const (
	Invalid SupportedDebeziumType = "invalid"
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

var typesThatRequireTypeCasting = []SupportedDebeziumType{
	Timestamp,
	MicroTimestamp,
	Date,
	Time,
	MicroTime,
	DateKafkaConnect,
	TimeKafkaConnect,
	DateTimeKafkaConnect,
	KafkaDecimalType,
	KafkaVariableNumericType,
	JSON,
	GeometryPointType,
	GeometryType,
	GeographyType,
}

func RequiresSpecialTypeCasting(typeLabel SupportedDebeziumType) (bool, SupportedDebeziumType) {
	for _, supportedType := range typesThatRequireTypeCasting {
		if typeLabel == supportedType {
			return true, supportedType
		}
	}

	return false, Invalid
}

// toBytes attempts to convert a value of unknown type to a slice of bytes.
// - If value is already a slice of bytes it will be directly returned.
// - If value is a string we will attempt to base64 decode it.
// - If value is any other type we will convert it to a string and then attempt to base64 decode it.
func ToBytes(value any) ([]byte, error) {
	if bytes, ok := value.([]byte); ok {
		return bytes, nil
	}

	var stringVal string
	if str, ok := value.(string); ok {
		stringVal = str
	} else {
		// TODO: Make this a hard error if we don't observe this happening.
		slog.Error("Expected string/[]byte, falling back to string",
			slog.String("type", fmt.Sprintf("%T", value)),
			slog.Any("value", fmt.Sprintf("%T", value)),
		)
		stringVal = fmt.Sprint(value)
	}

	data, err := base64.StdEncoding.DecodeString(stringVal)
	if err != nil {
		return nil, fmt.Errorf("failed to base64 decode: %w", err)
	}
	return data, nil
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
	return DecodeDecimal(encoded, results.Precision, results.Scale)
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

	bytes, err := ToBytes(val)
	if err != nil {
		return nil, err
	}

	f.Parameters = map[string]any{
		"scale":                  scale,
		KafkaDecimalPrecisionKey: "-1",
	}

	return f.DecodeDecimal(bytes)
}
