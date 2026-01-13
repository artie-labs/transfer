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
	expected := map[time.Time]string{
		time.Date(2025, 1, 12, 14, 30, 45, 0, time.UTC):         "14:30:45",
		time.Date(2025, 1, 12, 14, 30, 45, 123456000, time.UTC): "14:30:45.123456",
		time.Date(2025, 1, 12, 0, 0, 0, 0, time.UTC):            "00:00:00",
		time.Date(2025, 1, 12, 23, 59, 59, 999999000, time.UTC): "23:59:59.999999",
	}

	for expectedTime, expectedString := range expected {
		extTime := NewTime(expectedTime)
		assert.Equal(t, expectedString, extTime.String(), expectedTime)
	}
}

func TestTime_MarshalJSON(t *testing.T) {
	extTime := NewTime(time.Date(2025, 1, 12, 14, 30, 45, 123456000, time.UTC))

	data, err := json.Marshal(extTime)
	assert.NoError(t, err)
	assert.Equal(t, `"14:30:45.123456"`, string(data))
}
