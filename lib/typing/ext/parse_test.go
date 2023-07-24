package ext

import (
	"time"

	"github.com/artie-labs/transfer/lib/config"

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
		extTime, err := ParseFromInterface(e.ctx, val)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), val, extTime)
	}

	invalidVals := []interface{}{
		nil,
		true,
		false,
	}
	for _, invalidVal := range invalidVals {
		_, err := ParseFromInterface(e.ctx, invalidVal)
		assert.Error(e.T(), err)
	}
}

func (e *ExtTestSuite) TestParseFromInterfaceDateTime() {
	now := time.Now().In(time.UTC)
	for _, supportedDateTimeLayout := range supportedDateTimeLayouts {
		et, err := ParseFromInterface(e.ctx, now.Format(supportedDateTimeLayout))
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), et.NestedKind.Type, DateTimeKindType)
		assert.Equal(e.T(), et.String(""), now.Format(supportedDateTimeLayout))
	}
}

func (e *ExtTestSuite) TestParseFromInterfaceTime() {
	now := time.Now()
	for _, supportedTimeFormat := range supportedTimeFormats {
		et, err := ParseFromInterface(e.ctx, now.Format(supportedTimeFormat))
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), et.NestedKind.Type, TimeKindType)
		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(e.T(), et.String(""), now.Format(supportedTimeFormat))
	}
}

func (e *ExtTestSuite) TestParseFromInterfaceDate() {
	now := time.Now()
	for _, supportedDateFormat := range supportedDateFormats {
		et, err := ParseFromInterface(e.ctx, now.Format(supportedDateFormat))
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), et.NestedKind.Type, DateKindType)

		// Without passing an override format, this should return the same preserved dt format.
		assert.Equal(e.T(), et.String(""), now.Format(supportedDateFormat))
	}
}

func (e *ExtTestSuite) TestParseExtendedDateTime_Timestamp() {
	tsString := "2023-04-24T17:29:05.69944Z"
	extTime, err := ParseExtendedDateTime(e.ctx, tsString)
	assert.NoError(e.T(), err)
	assert.Equal(e.T(), "2023-04-24T17:29:05.69944Z", extTime.String(""))
}

func (e *ExtTestSuite) TestParseExtendedDateTime() {
	ctx := config.InjectSettingsIntoContext(e.ctx, &config.Settings{
		Config: &config.Config{
			SharedTransferConfig: config.SharedTransferConfig{
				// DD-MM-YY
				AdditionalDateFormats: []string{"02/01/06"},
			},
		},
	})

	dateString := "27/12/82"
	extTime, err := ParseExtendedDateTime(ctx, dateString)
	assert.NoError(e.T(), err)
	assert.Equal(e.T(), "27/12/82", extTime.String(""))
}

func (e *ExtTestSuite) TestTimeLayout() {
	ts := time.Now()

	for _, supportedFormat := range supportedTimeFormats {
		parsedTsString := ts.Format(supportedFormat)
		extTime, err := ParseExtendedDateTime(e.ctx, parsedTsString)
		assert.NoError(e.T(), err)
		assert.Equal(e.T(), parsedTsString, extTime.String(""))
	}
}
