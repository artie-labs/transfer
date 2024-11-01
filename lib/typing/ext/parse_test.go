package ext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseDateFromInterface(t *testing.T) {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		_time, err := ParseDateFromAny(now.Format(supportedDateFormat))
		assert.NoError(t, err)
		assert.Equal(t, _time.Format(supportedDateFormat), now.Format(supportedDateFormat))
	}
}

func TestParseTimeFromInterface(t *testing.T) {
	now := time.Now()
	{
		// String
		for _, supportedTimeFormat := range SupportedTimeFormats {
			_time, err := ParseTimeFromInterface(now.Format(supportedTimeFormat))
			assert.NoError(t, err)
			assert.Equal(t, _time.Format(supportedTimeFormat), now.Format(supportedTimeFormat))
		}
	}
	{
		// time.Time
		_time, err := ParseTimeFromInterface(now)
		assert.NoError(t, err)
		assert.Equal(t, now, _time)
	}
}

func TestParseTimestampTZFromInterface(t *testing.T) {
	{
		// Nil
		_, err := ParseTimestampTZFromInterface(nil)
		assert.ErrorContains(t, err, "val is nil")
	}
	{
		// Boolean
		{
			// True
			_, err := ParseTimestampTZFromInterface(true)
			assert.ErrorContains(t, err, "unsupported type: bool")
		}
		{
			// False
			_, err := ParseTimestampTZFromInterface(false)
			assert.ErrorContains(t, err, "unsupported type: bool")
		}
	}
	{
		// time.Time
		value, err := ParseTimestampTZFromInterface(time.Date(2024, 9, 19, 16, 5, 18, 123_456_789, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.123456789Z", value.Format(time.RFC3339Nano))
	}
	{
		// String - RFC3339MillisecondUTC
		value, err := ParseTimestampTZFromInterface("2024-09-19T16:05:18.631Z")
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.631Z", value.Format(time.RFC3339Nano))
	}
	{
		// String - RFC3339MicrosecondUTC
		value, err := ParseTimestampTZFromInterface("2024-09-19T16:05:18.630001Z")
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630001Z", value.Format(time.RFC3339Nano))
	}
	{
		// String - RFC3339NanosecondUTC
		value, err := ParseTimestampTZFromInterface("2024-09-19T16:05:18.630000002Z")
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630000002Z", value.Format(time.RFC3339Nano))
	}
}

func TestParseTimestampNTZFromInterface(t *testing.T) {
	{
		// No fractional seconds
		tsString := "2023-04-24T17:29:05"
		ts, err := ParseTimestampNTZFromInterface(tsString)
		assert.NoError(t, err)
		assert.Equal(t, tsString, ts.Format(RFC3339NoTZ))
	}
	{
		// ms
		tsString := "2023-04-24T17:29:05.123"
		ts, err := ParseTimestampNTZFromInterface(tsString)
		assert.NoError(t, err)
		assert.Equal(t, tsString, ts.Format(RFC3339NoTZ))
	}
	{
		// microseconds
		tsString := "2023-04-24T17:29:05.123456"
		ts, err := ParseTimestampNTZFromInterface(tsString)
		assert.NoError(t, err)
		assert.Equal(t, tsString, ts.Format(RFC3339NoTZ))
	}
	{
		// ns
		tsString := "2023-04-24T17:29:05.123456789"
		ts, err := ParseTimestampNTZFromInterface(tsString)
		assert.NoError(t, err)
		assert.Equal(t, tsString, ts.Format(RFC3339NoTZ))
	}
}
