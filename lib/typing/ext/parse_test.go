package ext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseFromInterface(t *testing.T) {
	{
		// Extended time
		var vals []*ExtendedTime
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimeKindType, PostgresTimeFormat))
		for _, val := range vals {
			_time, err := ParseFromInterface(val, TimeKindType)
			assert.NoError(t, err)
			assert.Equal(t, val.GetTime(), _time)
		}
	}
}

func TestParseFromInterfaceTime(t *testing.T) {
	now := time.Now()
	for _, supportedTimeFormat := range SupportedTimeFormats {
		_time, err := ParseFromInterface(now.Format(supportedTimeFormat), TimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, _time.Format(supportedTimeFormat), now.Format(supportedTimeFormat))
	}
}

func TestParseDateFromInterface(t *testing.T) {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		_time, err := ParseDateFromInterface(now.Format(supportedDateFormat))
		assert.NoError(t, err)
		assert.Equal(t, _time.Format(supportedDateFormat), now.Format(supportedDateFormat))
	}
}

func TestParseTimestampTZFromInterface(t *testing.T) {
	{
		// Nil
		_, err := ParseTimestampTZFromInterface(nil)
		assert.ErrorContains(t, err, "val is nil")
	}
	{
		// True
		_, err := ParseTimestampTZFromInterface(true)
		assert.ErrorContains(t, err, "failed to parse colVal, expected type string or *ExtendedTime and got: bool")
	}
	{
		// False
		_, err := ParseTimestampTZFromInterface(false)
		assert.ErrorContains(t, err, "failed to parse colVal, expected type string or *ExtendedTime and got: bool")
	}
	{
		// String - RFC3339MillisecondUTC
		value, err := ParseTimestampTZFromInterface("2024-09-19T16:05:18.630Z")
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630Z", value.Format(time.RFC3339Nano))
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

func TestTimeLayout(t *testing.T) {
	ts := time.Now()

	for _, supportedFormat := range SupportedTimeFormats {
		parsedTsString := ts.Format(supportedFormat)
		extTime, err := ParseDateTime(parsedTsString, TimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, parsedTsString, extTime.Format(supportedFormat))
	}
}
