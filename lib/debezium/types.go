package debezium

import (
	"fmt"
	"time"
)

type SupportedDebeziumType string

const (
	Invalid        SupportedDebeziumType = "invalid"
	MicroTimestamp SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
	Date           SupportedDebeziumType = "io.debezium.time.Date"
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

func FromDebeziumTypeToTime(supportedType SupportedDebeziumType, val int64) time.Time {
	// https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-temporal-types
	switch supportedType {
	case MicroTimestamp:
		// Cast the TZ in UTC. By default, it would be in local (machine) TZ.
		return time.UnixMicro(val).In(time.UTC)
	case Date:
		// Represents the number of days since the epoch.
		fmt.Println("time.UnixMicro(0).AddDate(0, 0, int(val))", time.UnixMicro(0).AddDate(0, 0, int(val)))
		return time.UnixMicro(0).AddDate(0, 0, int(val)).In(time.UTC)
	}

	return time.Time{}
}
