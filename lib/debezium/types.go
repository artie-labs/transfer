package debezium

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"time"
)

type SupportedDebeziumType string

const (
	Invalid        SupportedDebeziumType = "invalid"
	Timestamp      SupportedDebeziumType = "io.debezium.time.Timestamp"
	MicroTimestamp SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
	Date           SupportedDebeziumType = "io.debezium.time.Date"
	Time           SupportedDebeziumType = "io.debezium.time.Time"
	TimeMicro      SupportedDebeziumType = "io.debezium.time.MicroTime"
)

var supportedTypes = []SupportedDebeziumType{
	Timestamp,
	MicroTimestamp,
	Date,
	Time,
	TimeMicro,
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
	case Timestamp:
		// Represents the number of milliseconds since the epoch, and does not include timezone information.
		return ext.NewExtendedTime(time.UnixMilli(val).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case MicroTimestamp:
		// Represents the number of microseconds since the epoch, and does not include timezone information.
		return ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.DateTimeKindType, time.RFC3339Nano)
	case Date:
		// Represents the number of days since the epoch.
		return ext.NewExtendedTime(time.UnixMicro(0).AddDate(0, 0, int(val)).In(time.UTC), ext.DateKindType, "")
	case Time:
		// Represents the number of milliseconds past midnight, and does not include timezone information.
		return ext.NewExtendedTime(time.UnixMilli(val).In(time.UTC), ext.TimeKindType, "")
	case TimeMicro:
		// Represents the number of microseconds past midnight, and does not include timezone information.
		return ext.NewExtendedTime(time.UnixMicro(val).In(time.UTC), ext.TimeKindType, "")
	}

	return nil, fmt.Errorf("supportedType: %s, val: %v failed to be matched", supportedType, val)
}
