package ext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseDateFromAny(t *testing.T) {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		_time, err := ParseDateFromAny(now.Format(supportedDateFormat))
		assert.NoError(t, err)
		assert.Equal(t, _time.Format(supportedDateFormat), now.Format(supportedDateFormat))
	}
}

func TestParseTimeFromAny(t *testing.T) {
	now := time.Now()
	{
		// String
		for _, supportedTimeFormat := range SupportedTimeFormats {
			_time, err := ParseTimeFromAny(now.Format(supportedTimeFormat))
			assert.NoError(t, err)
			assert.Equal(t, _time.Format(supportedTimeFormat), now.Format(supportedTimeFormat))
		}
	}
	{
		// String
		_time, err := ParseTimeFromAny("2025-04-16T17:43:16.120+00:00")
		assert.NoError(t, err)
		assert.Equal(t, "2025-04-16T17:43:16.12Z", _time.Format(time.RFC3339Nano))
	}
	{
		// time.Time
		_time, err := ParseTimeFromAny(now)
		assert.NoError(t, err)
		assert.Equal(t, now, _time)
	}
}

func TestParseTimestampTZFromAny(t *testing.T) {
	{
		// Nil
		_, err := ParseTimestampTZFromAny(nil)
		assert.ErrorContains(t, err, "val is nil")
	}
	{
		// Boolean
		{
			// True
			_, err := ParseTimestampTZFromAny(true)
			assert.ErrorContains(t, err, "unsupported type: bool")
		}
		{
			// False
			_, err := ParseTimestampTZFromAny(false)
			assert.ErrorContains(t, err, "unsupported type: bool")
		}
	}
	{
		// time.Time
		value, err := ParseTimestampTZFromAny(time.Date(2024, 9, 19, 16, 5, 18, 123_456_789, time.UTC))
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.123456789Z", value.Format(time.RFC3339Nano))
	}
	{
		// String - RFC3339MillisecondUTC
		value, err := ParseTimestampTZFromAny("2024-09-19T16:05:18.631Z")
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.631Z", value.Format(time.RFC3339Nano))
	}
	{
		// String - RFC3339MicrosecondUTC
		value, err := ParseTimestampTZFromAny("2024-09-19T16:05:18.630001Z")
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630001Z", value.Format(time.RFC3339Nano))
	}
	{
		// String - RFC3339NanosecondUTC
		value, err := ParseTimestampTZFromAny("2024-09-19T16:05:18.630000002Z")
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630000002Z", value.Format(time.RFC3339Nano))
	}
	{
		// Another string variant
		value, err := ParseTimestampTZFromAny("2023-07-20T11:01:33.159+00:00")
		assert.NoError(t, err)
		assert.Equal(t, "2023-07-20T11:01:33.159Z", value.Format(time.RFC3339Nano))
	}
}

func TestParseTimestampNTZFromAny(t *testing.T) {
	{
		// No fractional seconds
		tsString := "2023-04-24T17:29:05"
		ts, err := ParseTimestampNTZFromAny(tsString)
		assert.NoError(t, err)
		assert.Equal(t, tsString, ts.Format(RFC3339NoTZ))
	}
	{
		// ms
		tsString := "2023-04-24T17:29:05.123"
		ts, err := ParseTimestampNTZFromAny(tsString)
		assert.NoError(t, err)
		assert.Equal(t, tsString, ts.Format(RFC3339NoTZ))
	}
	{
		// microseconds
		tsString := "2023-04-24T17:29:05.123456"
		ts, err := ParseTimestampNTZFromAny(tsString)
		assert.NoError(t, err)
		assert.Equal(t, tsString, ts.Format(RFC3339NoTZ))
	}
	{
		// ns
		tsString := "2023-04-24T17:29:05.123456789"
		ts, err := ParseTimestampNTZFromAny(tsString)
		assert.NoError(t, err)
		assert.Equal(t, tsString, ts.Format(RFC3339NoTZ))
	}
}
