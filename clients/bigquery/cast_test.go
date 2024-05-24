package bigquery

import (
	"fmt"
	"math/big"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing/decimal"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"
)

func (b *BigQueryTestSuite) TestCastColVal() {
	{
		// Integers
		colVal, err := castColVal(5, columns.Column{KindDetails: typing.Integer}, nil)
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), 5, colVal)
	}
	{
		// Floats
		colVal, err := castColVal(5.55, columns.Column{KindDetails: typing.Float}, nil)
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), 5.55, colVal)
	}
	{
		// Booleans
		colVal, err := castColVal(true, columns.Column{KindDetails: typing.Boolean}, nil)
		assert.NoError(b.T(), err)
		assert.True(b.T(), colVal.(bool))
	}
	{
		// EDecimals
		dec := decimal.NewDecimal(ptr.ToInt(5), 2, big.NewFloat(123.45))
		colVal, err := castColVal(dec, columns.Column{KindDetails: typing.EDecimal}, nil)
		assert.NoError(b.T(), err)

		// Native type is big.Float if precision doesn't exceed the DWH limit
		assert.Equal(b.T(), big.NewFloat(123.45), colVal)

		// Precision has clearly exceeded the limit, so we should be returning a string
		dec = decimal.NewDecimal(ptr.ToInt(50), 2, big.NewFloat(123.45))
		colVal, err = castColVal(dec, columns.Column{KindDetails: typing.EDecimal}, nil)
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), "123.45", colVal)
	}
	{
		// ETime
		birthday := time.Date(2022, time.September, 6, 3, 19, 24, 942000000, time.UTC)

		tsKind := typing.ETime
		tsKind.ExtendedTimeDetails = &ext.DateTime

		dateKind := typing.ETime
		dateKind.ExtendedTimeDetails = &ext.Date
		birthdayTSExt := ext.NewExtendedTime(birthday, tsKind.ExtendedTimeDetails.Type, "")
		{
			// Timestamp
			colVal, err := castColVal(birthdayTSExt, columns.Column{KindDetails: tsKind}, nil)
			assert.NoError(b.T(), err)
			assert.Equal(b.T(), "2022-09-06 03:19:24.942", colVal)
		}
		{
			// Date
			birthdayDateExt := ext.NewExtendedTime(birthday, dateKind.ExtendedTimeDetails.Type, "")
			colVal, err := castColVal(birthdayDateExt, columns.Column{KindDetails: dateKind}, nil)
			assert.NoError(b.T(), err)
			assert.Equal(b.T(), "2022-09-06", colVal)
		}

		{
			// Date (column is a date, but value is not)
			colVal, err := castColVal(birthdayTSExt, columns.Column{KindDetails: dateKind}, nil)
			assert.NoError(b.T(), err)
			assert.Equal(b.T(), "2022-09-06", colVal)
		}
		{
			// Time
			timeKind := typing.ETime
			timeKind.ExtendedTimeDetails = &ext.Time
			birthdayTimeExt := ext.NewExtendedTime(birthday, timeKind.ExtendedTimeDetails.Type, "")
			colVal, err := castColVal(birthdayTimeExt, columns.Column{KindDetails: timeKind}, nil)
			assert.NoError(b.T(), err)
			assert.Equal(b.T(), "03:19:24", colVal)
		}

		invalidDate := time.Date(0, time.September, 6, 3, 19, 24, 942000000, time.UTC)
		invalidDateTsExt := ext.NewExtendedTime(invalidDate, tsKind.ExtendedTimeDetails.Type, "")
		{
			// Date (column is a date, but value is invalid)
			colVal, err := castColVal(invalidDateTsExt, columns.Column{KindDetails: dateKind}, nil)
			assert.NoError(b.T(), err)
			assert.Nil(b.T(), colVal)
		}
		{
			// Datetime (column is datetime but value is invalid)
			colVal, err := castColVal(invalidDateTsExt, columns.Column{KindDetails: tsKind}, nil)
			assert.NoError(b.T(), err)
			assert.Nil(b.T(), colVal)
		}
	}
	{
		// Structs
		colVal, err := castColVal(map[string]any{"hello": "world"}, columns.Column{KindDetails: typing.Struct}, nil)
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), `{"hello":"world"}`, colVal)

		// With string values
		colVal, err = castColVal(`{"hello":"world"}`, columns.Column{KindDetails: typing.Struct}, nil)
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), `{"hello":"world"}`, colVal)

		// With empty string
		colVal, err = castColVal("", columns.Column{KindDetails: typing.Struct}, nil)
		assert.NoError(b.T(), err)
		assert.Nil(b.T(), colVal)

		// With array
		colVal, err = castColVal([]any{map[string]any{}, map[string]any{"hello": "world"}}, columns.Column{KindDetails: typing.Struct}, nil)
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), `[{},{"hello":"world"}]`, colVal)

		// With TOAST values
		colVal, err = castColVal(constants.ToastUnavailableValuePlaceholder, columns.Column{KindDetails: typing.Struct}, nil)
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder), colVal)
	}
	{
		// Arrays
		colVal, err := castColVal([]any{1, 2, 3, 4, 5}, columns.Column{KindDetails: typing.Array}, nil)
		assert.NoError(b.T(), err)
		assert.Equal(b.T(), []string{"1", "2", "3", "4", "5"}, colVal)

		// Empty array
		colVal, err = castColVal([]any{}, columns.Column{KindDetails: typing.Array}, nil)
		assert.NoError(b.T(), err)
		assert.Nil(b.T(), colVal)

		// Null array
		colVal, err = castColVal(nil, columns.Column{KindDetails: typing.Array}, nil)
		assert.NoError(b.T(), err)
		assert.Nil(b.T(), colVal)
	}
}
