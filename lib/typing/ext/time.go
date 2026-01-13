package ext

import (
	"encoding/json"
	"time"
)

const PostgresTimeFormatNoTZ = "15:04:05.999999"

// Time is Artie's wrapper around [time.Time] for TIME values (without date component).
// This allows us to control how time values are formatted when writing to destinations.
type Time struct {
	value time.Time
}

func NewTime(value time.Time) Time {
	return Time{value: value}
}

func (t Time) Value() time.Time {
	return t.value
}

// String returns the time formatted without timezone (e.g., "15:04:05.999999").
// This is used when writing values to destinations.
func (t Time) String() string {
	return t.value.Format(PostgresTimeFormatNoTZ)
}

func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}
