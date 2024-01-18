package ext

import (
	"time"

	"github.com/stretchr/testify/assert"
)

func (e *ExtTestSuite) TestParseFromInterface() {
	var vals []interface{}
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
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), val, extTime)
	}

	invalidVals := []interface{}{
		nil,
		true,
		false,
	}
	for _, invalidVal := range invalidVals {
		_, err := ParseFromInterface(invalidVal, nil)
		assert.Error(e.T(), err)
	}
}

func (e *ExtTestSuite) TestParseFromInterfaceDateTime() {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		et, err := ParseFromInterface(now.Format(supportedDateTimeLayout), nil)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), et.NestedKind.Type, DateTimeKindType)
		assert.Equal(e.T(), et.String(""), now.Format(supportedDateTimeLayout))
	}
}

func (e *ExtTestSuite) TestParseFromInterfaceTime() {
	now := time.Now()
	for _, supportedTimeFormat := range supportedTimeFormats {
		et, err := ParseFromInterface(now.Format(supportedTimeFormat), nil)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), et.NestedKind.Type, TimeKindType)
		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(e.T(), et.String(""), now.Format(supportedTimeFormat))
	}
}

func (e *ExtTestSuite) TestParseFromInterfaceDate() {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		et, err := ParseFromInterface(now.Format(supportedDateFormat), nil)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), et.NestedKind.Type, DateKindType)

		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(e.T(), et.String(""), now.Format(supportedDateFormat))
	}
}

func (e *ExtTestSuite) TestParseExtendedDateTime_Timestamp() {
	tsString := "2023-04-24T17:29:05.69944Z"
	extTime, err := ParseExtendedDateTime(tsString, nil)
	assert.NoError(e.T(), err)
	assert.Equal(e.T(), "2023-04-24T17:29:05.69944Z", extTime.String(""))
}

func (e *ExtTestSuite) TestParseExtendedDateTime() {
	dateString := "27/12/82"
	extTime, err := ParseExtendedDateTime(dateString, []string{"02/01/06"})
	assert.NoError(e.T(), err)
	assert.Equal(e.T(), "27/12/82", extTime.String(""))
}

func (e *ExtTestSuite) TestTimeLayout() {
	ts := time.Now()

	for _, supportedFormat := range supportedTimeFormats {
		parsedTsString := ts.Format(supportedFormat)
		extTime, err := ParseExtendedDateTime(parsedTsString, nil)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), parsedTsString, extTime.String(""))
	}
}
