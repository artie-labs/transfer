package ext

import (
	"fmt"
	"log/slog"
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

// MarshalJSON is a custom JSON marshaller for ExtendedTime.
// This is only used for nested MongoDB objects where there may be nested DateTime values.
func (e ExtendedTime) MarshalJSON() ([]byte, error) {
	// Delete this function once we delete [ExtendedTime].
	// This should not be happening anymore since MongoDB is now using [time.Time]
	slog.Error("Unexpected call to MarshalJSON()", slog.Any("ts", e.ts), slog.String("nestedKindType", string(e.nestedKind.Type)))
	return e.ts.UTC().MarshalJSON()
}

func (e *ExtendedTime) GetTime() time.Time {
	return e.ts
}

func (e *ExtendedTime) GetNestedKind() NestedKind {
	return e.nestedKind
}
