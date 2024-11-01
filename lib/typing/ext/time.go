package ext

import (
	"fmt"
	"time"
)

type ExtendedTimeKindType string

const (
	TimeKindType ExtendedTimeKindType = "time"
)

func (e ExtendedTimeKindType) defaultLayout() (string, error) {
	switch e {
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

// ExtendedTime is created because Golang's time.Time does not allow us to explicitly cast values as a date, or time
// and only allows timestamp expressions.
type ExtendedTime struct {
	ts         time.Time
	nestedKind NestedKind
}
