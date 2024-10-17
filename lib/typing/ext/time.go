package ext

import (
	"cmp"
	"encoding/json"
	"time"
)

// TODO: This package should have a concept of default formats for each type.

type ExtendedTimeKindType string

const (
	TimestampTzKindType  ExtendedTimeKindType = "timestamp_tz"
	TimestampNTZKindType ExtendedTimeKindType = "timestamp_ntz"
	DateKindType         ExtendedTimeKindType = "date"
	TimeKindType         ExtendedTimeKindType = "time"
)

type NestedKind struct {
	Type   ExtendedTimeKindType
	Format string
}

var (
	TimestampNTZ = NestedKind{
		Type:   TimestampNTZKindType,
		Format: RFC3339NanosecondNoTZ,
	}

	TimestampTz = NestedKind{
		Type:   TimestampTzKindType,
		Format: time.RFC3339Nano,
	}

	Date = NestedKind{
		Type:   DateKindType,
		Format: PostgresDateFormat,
	}

	Time = NestedKind{
		Type:   TimeKindType,
		Format: PostgresTimeFormat,
	}
)

// ExtendedTime is created because Golang's time.Time does not allow us to explicitly cast values as a date, or time
// and only allows timestamp expressions.
type ExtendedTime struct {
	ts         time.Time
	nestedKind NestedKind
}

func (e ExtendedTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.String(""))
}

func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) *ExtendedTime {
	if originalFormat == "" {
		switch kindType {
		case TimestampTzKindType:
			originalFormat = TimestampTz.Format
		case TimestampNTZKindType:
			originalFormat = TimestampNTZ.Format
		case DateKindType:
			originalFormat = Date.Format
		case TimeKindType:
			originalFormat = Time.Format
		}
	}

	return &ExtendedTime{
		ts: t,
		nestedKind: NestedKind{
			Type:   kindType,
			Format: originalFormat,
		},
	}
}

func (e *ExtendedTime) GetTime() time.Time {
	return e.ts
}

func (e *ExtendedTime) GetNestedKind() NestedKind {
	return e.nestedKind
}

func (e *ExtendedTime) String(overrideFormat string) string {
	format := cmp.Or(overrideFormat, e.nestedKind.Format)
	return e.ts.Format(format)
}
