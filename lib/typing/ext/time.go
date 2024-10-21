package ext

import (
	"cmp"
	"encoding/json"
	"fmt"
	"time"
)

type ExtendedTimeKindType string

const (
	TimestampTZKindType  ExtendedTimeKindType = "timestamp_tz"
	TimestampNTZKindType ExtendedTimeKindType = "timestamp_ntz"
	DateKindType         ExtendedTimeKindType = "date"
	TimeKindType         ExtendedTimeKindType = "time"
)

func (e ExtendedTimeKindType) defaultLayout() (string, error) {
	switch e {
	case TimestampTZKindType:
		return time.RFC3339Nano, nil
	case TimestampNTZKindType:
		return RFC3339NanosecondNoTZ, nil
	case DateKindType:
		return PostgresDateFormat, nil
	case TimeKindType:
		return PostgresTimeFormat, nil
	default:
		return "", fmt.Errorf("unknown kind type: %q", e)
	}
}

type NestedKind struct {
	Type   ExtendedTimeKindType
	Format string
}

var (
	TimestampNTZ = NestedKind{
		Type:   TimestampNTZKindType,
		Format: RFC3339NanosecondNoTZ,
	}

	TimestampTZ = NestedKind{
		Type:   TimestampTZKindType,
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

// MarshalJSON is a custom JSON marshaller for ExtendedTime. This is only by MongoDB where we are recursively parsing a nested object.
func (e ExtendedTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.ts)
}

// TODO: Have this return an error instead of nil
func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) *ExtendedTime {
	format, err := kindType.defaultLayout()
	if err != nil {
		return nil
	}

	return &ExtendedTime{
		ts: t,
		nestedKind: NestedKind{
			Type:   kindType,
			Format: cmp.Or(originalFormat, format),
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
