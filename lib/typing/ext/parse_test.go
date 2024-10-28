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
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimestampNTZKindType, RFC3339NoTZ))
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimestampTZKindType, ISO8601))
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimeKindType, PostgresTimeFormat))

		for _, val := range vals {
			_time, err := ParseFromInterface(val, TimestampTZKindType)
			assert.NoError(t, err)
			assert.Equal(t, val.GetTime(), _time)
		}
	}
	{
		// Nil
		_, err := ParseFromInterface(nil, TimestampTZKindType)
		assert.ErrorContains(t, err, "val is nil")
	}
	{
		// True
		_, err := ParseFromInterface(true, TimestampTZKindType)
		assert.ErrorContains(t, err, "failed to parse colVal, expected type string or *ExtendedTime and got: bool")
	}
	{
		// False
		_, err := ParseFromInterface(false, TimestampTZKindType)
		assert.ErrorContains(t, err, "failed to parse colVal, expected type string or *ExtendedTime and got: bool")
	}
	{
		// String - RFC3339MillisecondUTC
		value, err := ParseFromInterface("2024-09-19T16:05:18.630Z", TimestampTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630Z", value.Format(RFC3339Millisecond))
	}
	{
		// String - RFC3339MicrosecondUTC
		value, err := ParseFromInterface("2024-09-19T16:05:18.630000Z", TimestampTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630000Z", value.Format(RFC3339Microsecond))
	}
	{
		// String - RFC3339NanosecondUTC
		value, err := ParseFromInterface("2024-09-19T16:05:18.630000000Z", TimestampTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630000000Z", value.Format(RFC3339Nanosecond))
	}
}

func TestParseFromInterfaceDateTime(t *testing.T) {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		_time, err := ParseFromInterface(now.Format(supportedDateTimeLayout), TimestampTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, _time.Format(supportedDateTimeLayout), now.Format(supportedDateTimeLayout))
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

func TestParseExtendedDateTime_TimestampTZ(t *testing.T) {
	tsString := "2023-04-24T17:29:05.69944Z"
	extTime, err := ParseDateTime(tsString, TimestampTZKindType)
	assert.NoError(t, err)
	assert.Equal(t, tsString, extTime.Format(time.RFC3339Nano))
}

func TestParseExtendedDateTime_TimestampNTZ(t *testing.T) {
	{
		// No fractional seconds
		tsString := "2023-04-24T17:29:05"
		extTime, err := ParseDateTime(tsString, TimestampNTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, tsString, extTime.Format(RFC3339NoTZ))
	}
	{
		// ms
		tsString := "2023-04-24T17:29:05.123"
		extTime, err := ParseDateTime(tsString, TimestampNTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, tsString, extTime.Format(RFC3339NoTZ))
	}
	{
		// microseconds
		tsString := "2023-04-24T17:29:05.123456"
		extTime, err := ParseDateTime(tsString, TimestampNTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, tsString, extTime.Format(RFC3339NoTZ))
	}
	{
		// ns
		tsString := "2023-04-24T17:29:05.123456789"
		extTime, err := ParseDateTime(tsString, TimestampNTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, tsString, extTime.Format(RFC3339NoTZ))
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
