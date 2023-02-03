package debezium

import "time"

type SupportedDebeziumType string

const (
	Invalid        SupportedDebeziumType = "invalid"
	MicroTimestamp SupportedDebeziumType = "io.debezium.time.MicroTimestamp"
)

func RequiresSpecialTypeCasting(typeLabel string) (bool, SupportedDebeziumType) {
	for _, supportedType := range []SupportedDebeziumType{MicroTimestamp} {
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
	}

	return time.Time{}
}
