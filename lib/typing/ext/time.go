package ext

import "time"

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
		NestedKind: NestedKind{
			Type:   kindType,
			Format: originalFormat,
		},
	}, nil
}

func (e *ExtendedTime) String(overrideFormat string) string {
	if overrideFormat != "" {
		return e.Time.Format(overrideFormat)
	}

	return e.Time.Format(e.NestedKind.Format)
}
