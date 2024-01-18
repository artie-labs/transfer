package debezium

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"time"

	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/maputil"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

type SupportedDebeziumType string

const (
	Invalid SupportedDebeziumType = "invalid"
	JSON    SupportedDebeziumType = "io.debezium.data.Json"

	Timestamp            SupportedDebeziumType = "io.debezium.time.Timestamp"
	MicroTimestamp       SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
	Date                 SupportedDebeziumType = "io.debezium.time.Date"
	Time                 SupportedDebeziumType = "io.debezium.time.Time"
	TimeMicro            SupportedDebeziumType = "io.debezium.time.MicroTime"
	DateKafkaConnect     SupportedDebeziumType = "org.apache.kafka.connect.data.Date"
	TimeKafkaConnect     SupportedDebeziumType = "org.apache.kafka.connect.data.Time"
	TimeWithTimezone     SupportedDebeziumType = "io.debezium.time.ZonedTime"
	DateTimeWithTimezone SupportedDebeziumType = "io.debezium.time.ZonedTimestamp"
	DateTimeKafkaConnect SupportedDebeziumType = "org.apache.kafka.connect.data.Timestamp"

	KafkaDecimalType         SupportedDebeziumType = "org.apache.kafka.connect.data.Decimal"
	KafkaVariableNumericType SupportedDebeziumType = "io.debezium.data.VariableScaleDecimal"

	KafkaDecimalPrecisionKey = "connect.decimal.precision"
)

var typesThatRequireTypeCasting = []SupportedDebeziumType{
	Timestamp,
	MicroTimestamp,
	Date,
	Time,
	TimeMicro,
	DateKafkaConnect,
	TimeKafkaConnect,
	DateTimeKafkaConnect,
	KafkaDecimalType,
	KafkaVariableNumericType,
	JSON,
}

func RequiresSpecialTypeCasting(typeLabel string) (bool, SupportedDebeziumType) {
	for _, supportedType := range typesThatRequireTypeCasting {
		if typeLabel == string(supportedType) {
			return true, supportedType
		}
	}

	return false, Invalid
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
	case TimeMicro:
		// Represents the number of microseconds past midnight, and does not include timezone information.
		extTime, err = ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.TimeKindType, "")
	default:
		return nil, fmt.Errorf("supportedType: %s, val: %v failed to be matched", supportedType, val)
	}

	if err != nil {
		return nil, err
	}

	if extTime != nil {
		if !extTime.IsValid() {
			return nil, fmt.Errorf("extTime is not valid, extTime: %v", extTime)
		}
	}

	return extTime, err
}

// DecodeDecimal is used to handle `org.apache.kafka.connect.data.Decimal` where this would be emitted by Debezium when the `decimal.handling.mode` is `precise`
// * Encoded - takes the base64 encoded value
// * Parameters - which contains:
//   - `scale` (number of digits following decimal point)
//   - `connect.decimal.precision` which is an optional parameter. (If -1, then it's variable and .Value() will be in STRING).
func (f Field) DecodeDecimal(encoded string) (*decimal.Decimal, error) {
	results, err := f.GetScaleAndPrecision()
	if err != nil {
		return nil, fmt.Errorf("failed to get scale and/or precision, err: %v", err)
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to bae64 decode, err: %v", err)
	}

	bigInt := new(big.Int)

	// If the data represents a negative number, the sign bit will be set.
	if len(data) > 0 && data[0] >= 0x80 {
		// To convert the data to a two's complement integer, we need to invert the bytes and add one.
		for i := range data {
			data[i] = ^data[i]
		}

		bigInt.SetBytes(data)
		// We are adding this because Debezium (Java) encoded this value and uses two's complement binary representation for negative numbers
		bigInt.Add(bigInt, big.NewInt(1))
		bigInt.Neg(bigInt)
	} else {
		bigInt.SetBytes(data)
	}

	// Convert the big integer to a big float
	bigFloat := new(big.Float).SetInt(bigInt)

	// Compute divisor as 10^scale with big.Int's Exp, then convert to big.Float
	scaleInt := big.NewInt(int64(results.Scale))
	ten := big.NewInt(10)
	divisorInt := new(big.Int).Exp(ten, scaleInt, nil)
	divisorFloat := new(big.Float).SetInt(divisorInt)

	// Perform the division
	bigFloat.Quo(bigFloat, divisorFloat)
	return decimal.NewDecimal(results.Scale, results.Precision, bigFloat), nil
}

func (f Field) DecodeDebeziumVariableDecimal(value interface{}) (*decimal.Decimal, error) {
	valueStruct, isOk := value.(map[string]interface{})
	if !isOk {
		return nil, fmt.Errorf("value is not map[string]interface{} type")
	}

	scale, err := maputil.GetIntegerFromMap(valueStruct, "scale")
	if err != nil {
		return nil, err
	}

	val, isOk := valueStruct["value"]
	if !isOk {
		return nil, fmt.Errorf("encoded value does not exist")
	}

	f.Parameters = map[string]interface{}{
		"scale":                  scale,
		KafkaDecimalPrecisionKey: "-1",
	}

	return f.DecodeDecimal(fmt.Sprint(val))
}
