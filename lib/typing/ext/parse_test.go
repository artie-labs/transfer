package ext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseFromInterface(t *testing.T) {
	{
		// Extended time
		var vals []any
		vals = append(vals, NewExtendedTime(time.Now().UTC(), DateKindType, PostgresDateFormat))
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimestampTZKindType, ISO8601))
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimeKindType, PostgresTimeFormat))

		for _, val := range vals {
			extTime, err := ParseFromInterface(val, TimestampTZKindType)
			assert.NoError(t, err)
			assert.Equal(t, val, extTime)
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
		assert.Equal(t, "2024-09-19T16:05:18.630Z", value.String(""))
		assert.Equal(t, RFC3339MillisecondUTC, value.nestedKind.Format)
	}
	{
		// String - RFC3339MicrosecondUTC
		value, err := ParseFromInterface("2024-09-19T16:05:18.630000Z", TimestampTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630000Z", value.String(""))
		assert.Equal(t, RFC3339MicrosecondUTC, value.nestedKind.Format)
	}
	{
		// String - RFC3339NanosecondUTC
		value, err := ParseFromInterface("2024-09-19T16:05:18.630000000Z", TimestampTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, "2024-09-19T16:05:18.630000000Z", value.String(""))
		assert.Equal(t, RFC3339NanosecondUTC, value.nestedKind.Format)
	}
}

func TestParseFromInterfaceDateTime(t *testing.T) {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		et, err := ParseFromInterface(now.Format(supportedDateTimeLayout), TimestampTZKindType)
		assert.NoError(t, err)
		assert.Equal(t, TimestampTZKindType, et.GetNestedKind().Type)
		assert.Equal(t, et.String(""), now.Format(supportedDateTimeLayout))
	}
}

func TestParseFromInterfaceTime(t *testing.T) {
	now := time.Now()
	for _, supportedTimeFormat := range SupportedTimeFormats {
		et, err := ParseFromInterface(now.Format(supportedTimeFormat), TimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, TimeKindType, et.GetNestedKind().Type)
		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedTimeFormat))
	}
}

func TestParseFromInterfaceDate(t *testing.T) {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		et, err := ParseFromInterface(now.Format(supportedDateFormat), DateKindType)
		assert.NoError(t, err)
		assert.Equal(t, DateKindType, et.GetNestedKind().Type)

		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedDateFormat))
	}
}

func TestParseExtendedDateTime_Timestamp(t *testing.T) {
	tsString := "2023-04-24T17:29:05.69944Z"
	extTime, err := ParseExtendedDateTime(tsString, TimestampTZKindType)
	assert.NoError(t, err)
	assert.Equal(t, "2023-04-24T17:29:05.69944Z", extTime.String(""))
}

func TestTimeLayout(t *testing.T) {
	ts := time.Now()

	for _, supportedFormat := range SupportedTimeFormats {
		parsedTsString := ts.Format(supportedFormat)
		extTime, err := ParseExtendedDateTime(parsedTsString, TimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, parsedTsString, extTime.String(""))
	}
}
