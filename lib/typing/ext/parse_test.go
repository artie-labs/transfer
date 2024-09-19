package ext

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseFromInterface(t *testing.T) {
	{
		// Extended time
		var vals []any
		vals = append(vals, NewExtendedTime(time.Now().UTC(), DateKindType, PostgresDateFormat))
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimestampTzKindType, ISO8601))
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimeKindType, PostgresTimeFormat))

		for _, val := range vals {
			extTime, err := ParseFromInterface(val, TimestampTzKindType)
			assert.NoError(t, err)
			assert.Equal(t, val, extTime)
		}
	}
	{
		// Nil
		_, err := ParseFromInterface(nil, TimestampTzKindType)
		assert.ErrorContains(t, err, "val is nil")
	}
	{
		// True
		_, err := ParseFromInterface(true, TimestampTzKindType)
		assert.ErrorContains(t, err, "failed to parse colVal, expected type string or *ExtendedTime and got: bool")
	}
	{
		// False
		_, err := ParseFromInterface(false, TimestampTzKindType)
		assert.ErrorContains(t, err, "failed to parse colVal, expected type string or *ExtendedTime and got: bool")
	}
	{
		value, err := ParseFromInterface("2024-09-19T16:05:18.630Z", TimestampTzKindType)
		assert.NoError(t, err)
		fmt.Println("value", value)
	}
}

func TestParseFromInterfaceDateTime(t *testing.T) {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		et, err := ParseFromInterface(now.Format(supportedDateTimeLayout), TimestampTzKindType)
		assert.NoError(t, err)
		assert.Equal(t, TimestampTzKindType, et.GetNestedKind().Type)
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
	extTime, err := ParseExtendedDateTime(tsString, TimestampTzKindType)
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
