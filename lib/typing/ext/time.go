package ext

import (
	"cmp"
	"encoding/json"
	"fmt"
	"time"
)

// TODO: This package should have a concept of default formats for each type.

type ExtendedTimeKindType string

const (
	TimestampTzKindType ExtendedTimeKindType = "timestamp_tz"
	DateKindType        ExtendedTimeKindType = "date"
	TimeKindType        ExtendedTimeKindType = "time"
)

func NewExtendedTimeDetails(extendedType ExtendedTimeKindType, format string) (NestedKind, error) {
	var defaultFormat string
	switch extendedType {
	case TimestampTzKindType:
		defaultFormat = TimestampTz.Format
	case DateKindType:
		defaultFormat = Date.Format
	case TimeKindType:
		defaultFormat = Time.Format
	default:
		return NestedKind{}, fmt.Errorf("unsupported extended time kind type: %q", extendedType)
	}

	return NestedKind{Type: extendedType, Format: cmp.Or(format, defaultFormat)}, nil
}

type NestedKind struct {
	Type   ExtendedTimeKindType
	Format string
}

var (
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
	if overrideFormat != "" {
		return e.ts.Format(overrideFormat)
	}

	return e.ts.Format(e.nestedKind.Format)
}
