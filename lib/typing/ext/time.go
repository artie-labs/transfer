package ext

import (
	"cmp"
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

func NewNestedKind(kindType ExtendedTimeKindType, optionalFormat string) (NestedKind, error) {
	defaultLayout, err := kindType.defaultLayout()
	if err != nil {
		return NestedKind{}, err
	}

	return NestedKind{Type: kindType, Format: cmp.Or(optionalFormat, defaultLayout)}, nil
}

// ExtendedTime is created because Golang's time.Time does not allow us to explicitly cast values as a date, or time
// and only allows timestamp expressions.
type ExtendedTime struct {
	ts         time.Time
	nestedKind NestedKind
}

// MarshalJSON is a custom JSON marshaller for ExtendedTime.
// This is only used for nested MongoDB objects where there may be nested DateTime values.
func (e ExtendedTime) MarshalJSON() ([]byte, error) {
	// This is consistent with how MongoDB's Go driver marshals time.Time
	return e.ts.UTC().MarshalJSON()
}

// TODO: Have this return an error instead of nil
func NewExtendedTime(t time.Time, kindType ExtendedTimeKindType, originalFormat string) *ExtendedTime {
	defaultLayout, err := kindType.defaultLayout()
	if err != nil {
		return nil
	}

	return &ExtendedTime{
		ts: t,
		nestedKind: NestedKind{
			Type:   kindType,
			Format: cmp.Or(originalFormat, defaultLayout),
		},
	}
}

func (e *ExtendedTime) GetTime() time.Time {
	return e.ts
}

func (e *ExtendedTime) GetNestedKind() NestedKind {
	return e.nestedKind
}
