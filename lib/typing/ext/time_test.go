package ext

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewTime(t *testing.T) {
	now := time.Date(2025, 1, 12, 14, 30, 45, 123456000, time.UTC)
	extTime := NewTime(now)
	assert.Equal(t, now, extTime.Value())
}

func TestTime_String(t *testing.T) {
	tests := []struct {
		name     string
		time     time.Time
		expected string
	}{
		{
			name:     "basic time",
			time:     time.Date(2025, 1, 12, 14, 30, 45, 0, time.UTC),
			expected: "14:30:45",
		},
		{
			name:     "time with microseconds",
			time:     time.Date(2025, 1, 12, 14, 30, 45, 123456000, time.UTC),
			expected: "14:30:45.123456",
		},
		{
			name:     "midnight",
			time:     time.Date(2025, 1, 12, 0, 0, 0, 0, time.UTC),
			expected: "00:00:00",
		},
		{
			name:     "end of day",
			time:     time.Date(2025, 1, 12, 23, 59, 59, 999999000, time.UTC),
			expected: "23:59:59.999999",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			extTime := NewTime(tc.time)
			assert.Equal(t, tc.expected, extTime.String())
		})
	}
}

func TestTime_MarshalJSON(t *testing.T) {
	extTime := NewTime(time.Date(2025, 1, 12, 14, 30, 45, 123456000, time.UTC))

	data, err := json.Marshal(extTime)
	assert.NoError(t, err)
	assert.Equal(t, `"14:30:45.123456"`, string(data))
}
