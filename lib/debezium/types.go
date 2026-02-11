package debezium

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/artie-labs/transfer/lib/debezium/converters"
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
	JSON     SupportedDebeziumType = "io.debezium.data.Json"
	Enum     SupportedDebeziumType = "io.debezium.data.Enum"
	EnumSet  SupportedDebeziumType = "io.debezium.data.EnumSet"
	UUID     SupportedDebeziumType = "io.debezium.data.Uuid"
	LTree    SupportedDebeziumType = "io.debezium.data.Ltree"
	Interval SupportedDebeziumType = "io.debezium.time.Interval"
	XML      SupportedDebeziumType = "io.debezium.data.Xml"

	// Bytes
	Bits SupportedDebeziumType = "io.debezium.data.Bits"

	// Dates
	Date             SupportedDebeziumType = "io.debezium.time.Date"
	DateKafkaConnect SupportedDebeziumType = "org.apache.kafka.connect.data.Date"
	ZonedTimestamp   SupportedDebeziumType = "io.debezium.time.ZonedTimestamp"
	MicroDuration    SupportedDebeziumType = "io.debezium.time.MicroDuration"
	Year             SupportedDebeziumType = "io.debezium.time.Year"

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

	// Decimals
	KafkaDecimalType         SupportedDebeziumType = "org.apache.kafka.connect.data.Decimal"
	KafkaVariableNumericType SupportedDebeziumType = "io.debezium.data.VariableScaleDecimal"

	// PostGIS data types
	GeometryPointType SupportedDebeziumType = "io.debezium.data.geometry.Point"
	GeometryType      SupportedDebeziumType = "io.debezium.data.geometry.Geometry"
	GeographyType     SupportedDebeziumType = "io.debezium.data.geometry.Geography"

	KafkaDecimalPrecisionKey                       = "connect.decimal.precision"
	LineType                 SupportedDebeziumType = "io.debezium.data.LineString"
)

// toInt64 attempts to convert a value of unknown type to a an int64.
// - If the value is coming from Kafka it will be decoded as a float64 when it is unmarshalled from JSON.
// - If the value is coming from reader the value will be an int16/int32/int64.
func toInt64(value any) (int64, error) {
	switch typedValue := value.(type) {
	case int:
		return int64(typedValue), nil
	case int8:
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

func shouldSetDefaultTimeValue(t time.Time) bool {
	// Most of Debezium's time types uses Unix time, so we can check for zero value by comparing it to be zero.
	// If time value did not get set, it will return true for [IsZero]
	return t.Unix() != 0 && !t.IsZero()
}

// ShouldSetDefaultValue will filter out computed fields that cannot be properly set with a default value
func (f Field) ShouldSetDefaultValue(defaultValue any) bool {
	switch castedDefaultValue := defaultValue.(type) {
	case nil:
		return false
	case ext.Time:
		return shouldSetDefaultTimeValue(castedDefaultValue.Value())
	case time.Time:
		return shouldSetDefaultTimeValue(castedDefaultValue)
	case string:
		if f.DebeziumType == UUID && castedDefaultValue == uuid.Nil.String() {
			return false
		}

		return true
	case bool, int, int16, int32, int64, float32, float64, *decimal.Decimal:
		return true
	case map[string]any, []any:
		return true
	default:
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
	case Int8, Int16, Int32, Int64:
		var err error
		value, err = toInt64(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert to int64: %w", err)
		}
	case Bytes:
		var err error
		value, err = converters.Bytes{}.Convert(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert to bytes: %w", err)
		}
	}

	converter, err := f.ToValueConverter()
	if err != nil {
		return nil, fmt.Errorf("failed to convert to value converter: %w", err)
	}

	if converter != nil {
		return converter.Convert(value)
	}

	if bytes, ok := value.([]byte); ok {
		// Preserve existing behavior by base64 encoding []byte values to a string.
		// TODO: Look into inverting this logic so that in the case the field type is "bytes" but the value is a string we
		// base64 decode it in a preprocessing step above. Then things downstream from this method can just deal with []byte values.
		return base64.StdEncoding.EncodeToString(bytes), nil
	}

	return value, nil
}
