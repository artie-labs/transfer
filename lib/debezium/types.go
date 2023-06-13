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
	Invalid        SupportedDebeziumType = "invalid"
	Timestamp      SupportedDebeziumType = "io.debezium.time.Timestamp"
	MicroTimestamp SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
	Date           SupportedDebeziumType = "io.debezium.time.Date"
	Time           SupportedDebeziumType = "io.debezium.time.Time"
	TimeMicro      SupportedDebeziumType = "io.debezium.time.MicroTime"

	DateKafkaConnect     SupportedDebeziumType = "org.apache.kafka.connect.data.Date"
	TimeKafkaConnect     SupportedDebeziumType = "org.apache.kafka.connect.data.Time"
	DateTimeKafkaConnect SupportedDebeziumType = "org.apache.kafka.connect.data.Timestamp"

	KafkaDecimalType SupportedDebeziumType = "org.apache.kafka.connect.data.Decimal"
)

var supportedTypes = []SupportedDebeziumType{
	Timestamp,
	MicroTimestamp,
	Date,
	Time,
	TimeMicro,
	DateKafkaConnect,
	TimeKafkaConnect,
	DateTimeKafkaConnect,
	KafkaDecimalType,
}

func RequiresSpecialTypeCasting(typeLabel string) (bool, SupportedDebeziumType) {
	for _, supportedType := range supportedTypes {
		if typeLabel == string(supportedType) {
			return true, supportedType
		}
	}

	return false, Invalid
}

func FromDebeziumTypeToTime(supportedType SupportedDebeziumType, val int64) (*ext.ExtendedTime, error) {
	// https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-temporal-types
	switch supportedType {
	case Timestamp, DateTimeKafkaConnect:
		// Represents the number of milliseconds since the epoch, and does not include timezone information.
		return ext.NewExtendedTime(time.UnixMilli(val).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case MicroTimestamp:
		// Represents the number of microseconds since the epoch, and does not include timezone information.
		return ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case Date, DateKafkaConnect:
		unix := time.UnixMilli(0).In(time.UTC) // 1970-01-01
		// Represents the number of days since the epoch.
		return ext.NewExtendedTime(unix.AddDate(0, 0, int(val)), ext.DateKindType, "")
	case Time, TimeKafkaConnect:
		// Represents the number of milliseconds past midnight, and does not include timezone information.
		return ext.NewExtendedTime(time.UnixMilli(val).In(time.UTC), ext.TimeKindType, "")
	case TimeMicro:
		// Represents the number of microseconds past midnight, and does not include timezone information.
		return ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.TimeKindType, "")

	}

	return nil, fmt.Errorf("supportedType: %s, val: %v failed to be matched", supportedType, val)
}

// DecodeDecimal is used to handle `org.apache.kafka.connect.data.Decimal` where this would be emitted by Debezium when the `decimal.handling.mode` is `precise`
// Encoded - takes the encoded value
// Parameters - which contains: `scale` and `connect.decimal.precision`
// TODO: test.
func DecodeDecimal(encoded string, parameters map[string]interface{}) (*decimal.Decimal, error) {
	scale, scaleErr := maputil.GetIntegerFromMap(parameters, "scale")
	if scaleErr != nil {
		return nil, scaleErr
	}

	precision, precisionErr := maputil.GetIntegerFromMap(parameters, "connect.decimal.precision")
	if precisionErr != nil {
		return nil, precisionErr
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("failed to bae64 decode, err: %v", err)
	}

	bigInt := new(big.Int)
	bigInt.SetBytes(data)

	// Convert the big integer to a big float, and divide it by 10^scale
	bigFloat := new(big.Float).SetInt(bigInt)
	divisor := new(big.Float).SetFloat64(float64(1))
	for i := 0; i < scale; i++ {
		divisor.Mul(divisor, big.NewFloat(float64(10)))
	}

	bigFloat.Quo(bigFloat, divisor)

	return decimal.NewDecimal(scale, precision, bigFloat), nil
}
