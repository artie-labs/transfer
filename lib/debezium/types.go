package debezium

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/typing"
	"time"
)

type SupportedDebeziumType string

const (
	Invalid        SupportedDebeziumType = "invalid"
	MicroTimestamp SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
	Date           SupportedDebeziumType = "io.debezium.time.Date"
	Time           SupportedDebeziumType = "io.debezium.time.Time"
)

var supportedTypes = []SupportedDebeziumType{
	MicroTimestamp,
	Date,
}

func RequiresSpecialTypeCasting(typeLabel string) (bool, SupportedDebeziumType) {
	for _, supportedType := range supportedTypes {
		if typeLabel == string(supportedType) {
			return true, supportedType
		}
	}

	return false, Invalid
}

func FromDebeziumTypeToTime(supportedType SupportedDebeziumType, val int64) (*typing.ExtendedTime, error) {
	// https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-temporal-types
	switch supportedType {
	case MicroTimestamp:
		// Cast the TZ in UTC. By default, it would be in local (machine) TZ.
		return typing.NewExtendedTime(time.UnixMicro(val).In(time.UTC), typing.DateTimeKindType, "")
	case Date:
		// Represents the number of days since the epoch.
		return typing.NewExtendedTime(time.UnixMicro(0).AddDate(0, 0, int(val)).In(time.UTC), typing.DateKindType, "")
	case Time:
		// Represents the number of milliseconds past midnight, and does not include timezone information.
		return typing.NewExtendedTime(time.UnixMicro(val).In(time.UTC), typing.TimeKindType, "")
	}

	return nil, fmt.Errorf("supportedType: %s, val: %v failed to be matched", supportedType, val)
}
