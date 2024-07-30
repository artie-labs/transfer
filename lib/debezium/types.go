package debezium

import (
	"encoding/base64"
	"fmt"
	"time"

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

	Timestamp            SupportedDebeziumType = "io.debezium.time.Timestamp"
	MicroTimestamp       SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
	NanoTimestamp        SupportedDebeziumType = "io.debezium.time.NanoTimestamp"
	Date                 SupportedDebeziumType = "io.debezium.time.Date"
	Year                 SupportedDebeziumType = "io.debezium.time.Year"
	DateTimeWithTimezone SupportedDebeziumType = "io.debezium.time.ZonedTimestamp"
	MicroDuration        SupportedDebeziumType = "io.debezium.time.MicroDuration"
	DateKafkaConnect     SupportedDebeziumType = "org.apache.kafka.connect.data.Date"
	DateTimeKafkaConnect SupportedDebeziumType = "org.apache.kafka.connect.data.Timestamp"

	// All the possible time data types
	Time             SupportedDebeziumType = "io.debezium.time.Time"
	MicroTime        SupportedDebeziumType = "io.debezium.time.MicroTime"
	NanoTime         SupportedDebeziumType = "io.debezium.time.NanoTime"
	TimeWithTimezone SupportedDebeziumType = "io.debezium.time.ZonedTime"
	TimeKafkaConnect SupportedDebeziumType = "org.apache.kafka.connect.data.Time"

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
	}

	converter, err := f.ToValueConverter()
	if err != nil {
		return nil, fmt.Errorf("failed to get value converter: %w", err)
	}

	if converter != nil {
		return converter.Convert(value)
	}

	switch f.DebeziumType {
	case
		Timestamp,
		MicroTimestamp,
		NanoTimestamp,
		NanoTime,
		MicroTime,
		DateTimeKafkaConnect:
		int64Value, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64 got '%v' with type %T", value, value)
		}
		return FromDebeziumTypeToTime(f.DebeziumType, int64Value)
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
	case NanoTimestamp:
		// Represents the number of nanoseconds past the epoch, and does not include timezone information.
		extTime = ext.NewExtendedTime(time.UnixMicro(val/1_000).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case MicroTime:
		// Represents the number of microseconds past midnight, and does not include timezone information.
		extTime = ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.TimeKindType, "")
	case NanoTime:
		// Represents the number of nanoseconds past midnight, and does not include timezone information.
		extTime = ext.NewExtendedTime(time.UnixMicro(val/1_000).In(time.UTC), ext.TimeKindType, "")
	default:
		return nil, fmt.Errorf("supportedType: %s, val: %v failed to be matched", supportedType, val)
	}

	if extTime != nil && !extTime.IsValid() {
		return nil, fmt.Errorf("extTime is invalid: %v", extTime)
	}

	return extTime, nil
}
