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

	invalidVals := []any{
		nil,
		true,
		false,
	}
	for _, invalidVal := range invalidVals {
		_, err := ParseFromInterface(invalidVal, nil)
		assert.Error(t, err)
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
	dateString := "27/12/82"
	extTime, err := ParseExtendedDateTime(dateString, []string{"02/01/06"})
	assert.NoError(t, err)
	assert.Equal(t, "27/12/82", extTime.String(""))
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
