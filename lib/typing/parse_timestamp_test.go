package typing

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
	{
		// String value: 2025-10-26T09:37:07.350000+00:00
		_time, err := ParseDateFromAny("2025-10-26T09:37:07.350000+00:00")
		assert.NoError(t, err)
		assert.Equal(t, "2025-10-26", _time.Format(time.DateOnly))
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

func TestValidateTimestampYear(t *testing.T) {
	{
		// Valid years
		for _, year := range []int{1, 1000, 2024, 9999} {
			ts := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
			assert.NoError(t, ValidateTimestampYear(ts), "year %d should be valid", year)
		}
	}
	{
		// Year too large (e.g., 262025)
		ts := time.Date(262025, 10, 6, 7, 0, 0, 0, time.UTC)
		err := ValidateTimestampYear(ts)
		assert.ErrorContains(t, err, "year 262025 is out of range [1, 9999]")
		parseError, ok := BuildParseError(err)
		assert.True(t, ok)
		assert.Equal(t, YearOutOfRange, parseError.GetKind())
	}
	{
		// Year 0 (invalid)
		ts := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)
		err := ValidateTimestampYear(ts)
		assert.ErrorContains(t, err, "year 0 is out of range [1, 9999]")
	}
	{
		// Negative year (invalid)
		ts := time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC)
		err := ValidateTimestampYear(ts)
		assert.ErrorContains(t, err, "year -1 is out of range [1, 9999]")
	}
	{
		// Year 10000 (invalid)
		ts := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
		err := ValidateTimestampYear(ts)
		assert.ErrorContains(t, err, "year 10000 is out of range [1, 9999]")
	}
}
