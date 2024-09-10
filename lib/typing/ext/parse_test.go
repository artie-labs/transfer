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
		vals = append(vals, NewExtendedTime(time.Now().UTC(), DateTimeKindType, ISO8601))
		vals = append(vals, NewExtendedTime(time.Now().UTC(), TimeKindType, PostgresTimeFormat))

		for _, val := range vals {
			extTime, err := ParseFromInterfaceNew(val, DateTimeKindType)
			assert.NoError(t, err)
			assert.Equal(t, val, extTime)
		}
	}
	{
		// Nil
		_, err := ParseFromInterfaceNew(nil, DateTimeKindType)
		assert.ErrorContains(t, err, "val is nil")
	}
	{
		// True
		_, err := ParseFromInterfaceNew(true, DateTimeKindType)
		assert.ErrorContains(t, err, "failed to parse colVal, expected type string or *ExtendedTime and got: bool")
	}
	{
		// False
		_, err := ParseFromInterfaceNew(false, DateTimeKindType)
		assert.ErrorContains(t, err, "failed to parse colVal, expected type string or *ExtendedTime and got: bool")
	}
}

func TestParseFromInterfaceDateTime(t *testing.T) {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		et, err := ParseFromInterfaceNew(now.Format(supportedDateTimeLayout), DateTimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, DateTimeKindType, et.GetNestedKind().Type)
		assert.Equal(t, et.String(""), now.Format(supportedDateTimeLayout))
	}
}

func TestParseFromInterfaceTime(t *testing.T) {
	now := time.Now()
	for _, supportedTimeFormat := range SupportedTimeFormatsLegacy {
		et, err := ParseFromInterfaceNew(now.Format(supportedTimeFormat), TimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, TimeKindType, et.GetNestedKind().Type)
		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedTimeFormat))
	}
}

func TestParseFromInterfaceDate(t *testing.T) {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		et, err := ParseFromInterfaceNew(now.Format(supportedDateFormat), DateKindType)
		assert.NoError(t, err)
		assert.Equal(t, DateKindType, et.GetNestedKind().Type)

		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedDateFormat))
	}
}

func TestParseExtendedDateTime_Timestamp(t *testing.T) {
	tsString := "2023-04-24T17:29:05.69944Z"
	extTime, err := ParseExtendedDateTimeNew(tsString, DateTimeKindType)
	assert.NoError(t, err)
	assert.Equal(t, "2023-04-24T17:29:05.69944Z", extTime.String(""))
}

func TestTimeLayout(t *testing.T) {
	ts := time.Now()

	for _, supportedFormat := range SupportedTimeFormatsLegacy {
		parsedTsString := ts.Format(supportedFormat)
		extTime, err := ParseExtendedDateTimeNew(parsedTsString, TimeKindType)
		assert.NoError(t, err)
		assert.Equal(t, parsedTsString, extTime.String(""))
	}
}
