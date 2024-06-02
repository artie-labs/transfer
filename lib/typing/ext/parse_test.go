package ext

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseFromInterface(t *testing.T) {
	var vals []any
	vals = append(vals, &ExtendedTime{
		Time: time.Now().UTC(),
		NestedKind: NestedKind{
			Type:   DateKindType,
			Format: PostgresDateFormat,
		},
	}, &ExtendedTime{
		Time: time.Now().UTC(),
		NestedKind: NestedKind{
			Type:   DateTimeKindType,
			Format: ISO8601,
		},
	}, &ExtendedTime{
		Time: time.Now().UTC(),
		NestedKind: NestedKind{
			Type:   TimeKindType,
			Format: PostgresTimeFormat,
		},
	})

	for _, val := range vals {
		extTime, err := ParseFromInterface(val, nil)
		assert.NoError(t, err)
		assert.Equal(t, val, extTime)
	}

	{
		// Nil
		_, err := ParseFromInterface(nil, nil)
		assert.ErrorContains(t, err, "val is nil")
	}
	{
		// True
		_, err := ParseFromInterface(true, nil)
		assert.ErrorContains(t, err, "true is not supported")
	}
	{
		// False
		_, err := ParseFromInterface(false, nil)
		assert.ErrorContains(t, err, "false is not supported")
	}
}

func TestParseFromInterfaceDateTime(t *testing.T) {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		et, err := ParseFromInterface(now.Format(supportedDateTimeLayout), nil)
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, DateTimeKindType)
		assert.Equal(t, et.String(""), now.Format(supportedDateTimeLayout))
	}
}

func TestParseFromInterfaceTime(t *testing.T) {
	now := time.Now()
	for _, supportedTimeFormat := range supportedTimeFormats {
		et, err := ParseFromInterface(now.Format(supportedTimeFormat), nil)
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, TimeKindType)
		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedTimeFormat))
	}
}

func TestParseFromInterfaceDate(t *testing.T) {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		et, err := ParseFromInterface(now.Format(supportedDateFormat), nil)
		assert.NoError(t, err)
		assert.Equal(t, et.NestedKind.Type, DateKindType)

		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(t, et.String(""), now.Format(supportedDateFormat))
	}
}

func TestParseExtendedDateTime_Timestamp(t *testing.T) {
	tsString := "2023-04-24T17:29:05.69944Z"
	extTime, err := ParseExtendedDateTime(tsString, nil)
	assert.NoError(t, err)
	assert.Equal(t, "2023-04-24T17:29:05.69944Z", extTime.String(""))
}

func TestParseExtendedDateTime(t *testing.T) {
	{
		dateString := "27/12/82"
		extTime, err := ParseExtendedDateTime(dateString, []string{"02/01/06"})
		assert.NoError(t, err)
		assert.Equal(t, "27/12/82", extTime.String(""))
	}
	{
		dtString := "Mon Jan 02 15:04:05.69944 -0700 2006"
		ts, err := ParseExtendedDateTime(dtString, nil)
		assert.NoError(t, err)
		assert.NotEqual(t, ts.String(""), dtString)
	}
	{
		// Edge case
		dtString := "+275760-09-13T00:00:00.000000Z"
		ts, err := ParseExtendedDateTime(dtString, nil)
		assert.NoError(t, err)
		assert.Equal(t, "0001-01-01T00:00:00+00:00", ts.String(""))
	}
}

func TestTimeLayout(t *testing.T) {
	ts := time.Now()

	for _, supportedFormat := range supportedTimeFormats {
		parsedTsString := ts.Format(supportedFormat)
		extTime, err := ParseExtendedDateTime(parsedTsString, nil)
		assert.NoError(t, err)
		assert.Equal(t, parsedTsString, extTime.String(""))
	}
}
