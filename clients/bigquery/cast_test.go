package bigquery

import (
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
)

func (b *BigQueryTestSuite) TestCastColVal() {
	type _testCase struct {
		name    string
		colVal  interface{}
		colKind columns.Column

		expectedErr   error
		expectedValue interface{}
	}

	tsKind := typing.ETime
	tsKind.ExtendedTimeDetails = &ext.DateTime

	dateKind := typing.ETime
	dateKind.ExtendedTimeDetails = &ext.Date

	birthday := time.Date(2022, time.September, 6, 3, 19, 24, 942000000, time.UTC)
	birthdayTSExt, err := ext.NewExtendedTime(birthday, tsKind.ExtendedTimeDetails.Type, "")
	assert.NoError(b.T(), err)

	birthdayDateExt, err := ext.NewExtendedTime(birthday, dateKind.ExtendedTimeDetails.Type, "")
	assert.NoError(b.T(), err)

	timeKind := typing.ETime
	timeKind.ExtendedTimeDetails = &ext.Time
	birthdayTimeExt, err := ext.NewExtendedTime(birthday, timeKind.ExtendedTimeDetails.Type, "")
	assert.NoError(b.T(), err)

	invalidDate := time.Date(0, time.September, 6, 3, 19, 24, 942000000, time.UTC)
	invalidDateTsExt, err := ext.NewExtendedTime(invalidDate, tsKind.ExtendedTimeDetails.Type, "")
	assert.NoError(b.T(), err)

	testCases := []_testCase{
		{
			name:          "escaping string",
			colVal:        "foo",
			colKind:       columns.Column{KindDetails: typing.String},
			expectedValue: "foo",
		},
		{
			name:          "123 as int",
			colVal:        123,
			colKind:       columns.Column{KindDetails: typing.Integer},
			expectedValue: "123",
		},
		{
			name:          "struct",
			colVal:        `{"hello": "world"}`,
			colKind:       columns.Column{KindDetails: typing.Struct},
			expectedValue: `{"hello": "world"}`,
		},
		{
			name:          "struct w/ toast",
			colVal:        constants.ToastUnavailableValuePlaceholder,
			colKind:       columns.Column{KindDetails: typing.Struct},
			expectedValue: fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder),
		},
		{
			name:          "array",
			colVal:        []int{1, 2, 3, 4, 5},
			colKind:       columns.Column{KindDetails: typing.Array},
			expectedValue: []string{"1", "2", "3", "4", "5"},
		},
		{
			name:          "empty array",
			colVal:        []int{},
			colKind:       columns.Column{KindDetails: typing.Array},
			expectedValue: nil,
		},
		{
			name:          "null array",
			colVal:        nil,
			colKind:       columns.Column{KindDetails: typing.Array},
			expectedValue: nil,
		},
		{
			name:          "timestamp",
			colVal:        birthdayTSExt,
			colKind:       columns.Column{KindDetails: tsKind},
			expectedValue: "2022-09-06 03:19:24.942",
		},
		{
			name:          "date",
			colVal:        birthdayDateExt,
			colKind:       columns.Column{KindDetails: dateKind},
			expectedValue: "2022-09-06",
		},
		{
			name:          "date (column is a date, but value is not)",
			colVal:        birthdayTSExt,
			colKind:       columns.Column{KindDetails: dateKind},
			expectedValue: "2022-09-06",
		},
		{
			name:          "time",
			colVal:        birthdayTimeExt,
			colKind:       columns.Column{KindDetails: timeKind},
			expectedValue: "03:19:24",
		},
		{
			name:    "date (column is a date, but value is invalid)",
			colVal:  invalidDateTsExt,
			colKind: columns.Column{KindDetails: dateKind},
		},
		{
			name:    "datetime (column is a datetime, but value is invalid)",
			colVal:  invalidDateTsExt,
			colKind: columns.Column{KindDetails: tsKind},
		},
	}

	for _, testCase := range testCases {
		actualString, actualErr := CastColVal(b.ctx, testCase.colVal, testCase.colKind)
		assert.Equal(b.T(), testCase.expectedErr, actualErr, testCase.name)
		assert.Equal(b.T(), testCase.expectedValue, actualString, testCase.name)
	}
}
