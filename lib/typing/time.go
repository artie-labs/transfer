package typing

import (
	"time"
)

type ExtendedTimeKindType string

const (
	DateTimeKindType ExtendedTimeKindType = "datetime"
	DateKindType     ExtendedTimeKindType = "date"
	TimeKindType     ExtendedTimeKindType = "time"
)

type ExtendedTimeKind struct {
	Type   ExtendedTimeKindType
	Format string
}

var (
	DateTime = ExtendedTimeKind{
		Type:   DateTimeKindType,
		Format: time.RFC3339Nano,
	}

	Date = ExtendedTimeKind{
		Type:   DateKindType,
		Format: PostgresDateFormat,
	}

	Time = ExtendedTimeKind{
		Type:   TimeKindType,
		Format: PostgresTimeWithoutTZFormat,
	}
)

// ExtendedTime is created because Golang's time.Time does not allow us to explicitly cast values as a date, or time
// and only allows timestamp expressions.
type ExtendedTime struct {
	time.Time
	extendedTimeKind ExtendedTimeKind
}

func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) (*ExtendedTime, error) {
	if originalFormat == "" {
		switch kindType {
		case DateTimeKindType:
			originalFormat = DateTime.Format
			break
		case DateKindType:
			originalFormat = Date.Format
			break
		case TimeKindType:
			originalFormat = Time.Format
			break
		}
	}

	return &ExtendedTime{
		Time: t,
		extendedTimeKind: ExtendedTimeKind{
			Type:   kindType,
			Format: originalFormat,
		},
	}, nil
}

func (e *ExtendedTime) String(overrideFormat string) string {
	if overrideFormat != "" {
		return e.Time.Format(overrideFormat)
	}

	return e.Time.Format(e.extendedTimeKind.Format)
}
