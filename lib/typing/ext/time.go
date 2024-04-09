package ext

import (
	"time"
)

type ExtendedTimeKindType string

const (
	DateTimeKindType ExtendedTimeKindType = "datetime"
	DateKindType     ExtendedTimeKindType = "date"
	TimeKindType     ExtendedTimeKindType = "time"
)

type NestedKind struct {
	Type   ExtendedTimeKindType
	Format string
}

var (
	DateTime = NestedKind{
		Type:   DateTimeKindType,
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
	time.Time
	NestedKind NestedKind
}

func (e *ExtendedTime) IsValid() bool {
	// This will make us feature-parity with Go: https://github.com/golang/go/blob/97daa6e94296980b4aa2dac93a938a5edd95ce93/src/time/format_rfc3339.go#L62
	if e.Time.Year() > 9999 || e.Time.Year() < 0 {
		return false
	}

	return true
}

func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) *ExtendedTime {
	if originalFormat == "" {
		switch kindType {
		case DateTimeKindType:
			originalFormat = DateTime.Format
		case DateKindType:
			originalFormat = Date.Format
		case TimeKindType:
			originalFormat = Time.Format
		}
	}

	return &ExtendedTime{
		Time: t,
		NestedKind: NestedKind{
			Type:   kindType,
			Format: originalFormat,
		},
	}
}

func (e *ExtendedTime) GetTime() time.Time {
	return e.Time
}

func (e *ExtendedTime) String(overrideFormat string) string {
	if overrideFormat != "" {
		return e.Time.Format(overrideFormat)
	}

	return e.Time.Format(e.NestedKind.Format)
}

func (e *ExtendedTime) StringUTC(overrideFormat string) string {
	if overrideFormat != "" {
		return e.Time.In(time.UTC).Format(overrideFormat)
	}

	return e.Time.In(time.UTC).Format(e.NestedKind.Format)
}
