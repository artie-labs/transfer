package bigquery

import (
	"fmt"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
)

func TestCastColVal(t *testing.T) {
	type _testCase struct {
		name    string
		colVal  interface{}
		colKind typing.Column

		expectedErr   error
		expectedValue interface{}
	}

	tsKind := typing.ETime
	tsKind.ExtendedTimeDetails = &ext.DateTime

	dateKind := typing.ETime
	dateKind.ExtendedTimeDetails = &ext.Date

	birthday := time.Date(2022, time.September, 6, 3, 19, 24, 942000000, time.UTC)
	birthdayTSExt, err := ext.NewExtendedTime(birthday, tsKind.ExtendedTimeDetails.Type, "")
	assert.NoError(t, err)

	birthdayDateExt, err := ext.NewExtendedTime(birthday, dateKind.ExtendedTimeDetails.Type, "")
	assert.NoError(t, err)

	timeKind := typing.ETime
	timeKind.ExtendedTimeDetails = &ext.Time
	birthdayTimeExt, err := ext.NewExtendedTime(birthday, timeKind.ExtendedTimeDetails.Type, "")
	assert.NoError(t, err)

	testCases := []_testCase{
		{
			name:          "escaping string",
			colVal:        "foo",
			colKind:       typing.Column{KindDetails: typing.String},
			expectedValue: "foo",
		},
		{
			name:          "123 as int",
			colVal:        123,
			colKind:       typing.Column{KindDetails: typing.Integer},
			expectedValue: "123",
		},
		{
			name:          "struct",
			colVal:        `{"hello": "world"}`,
			colKind:       typing.Column{KindDetails: typing.Struct},
			expectedValue: `{"hello": "world"}`,
		},
		{
			name:          "struct w/ toast",
			colVal:        constants.ToastUnavailableValuePlaceholder,
			colKind:       typing.Column{KindDetails: typing.Struct},
			expectedValue: fmt.Sprintf(`{"key":"%s"}`, constants.ToastUnavailableValuePlaceholder),
		},
		{
			name:          "array",
			colVal:        []int{1, 2, 3, 4, 5},
			colKind:       typing.Column{KindDetails: typing.Array},
			expectedValue: []string{"1", "2", "3", "4", "5"},
		},
		{
			name:          "timestamp",
			colVal:        birthdayTSExt,
			colKind:       typing.Column{KindDetails: tsKind},
			expectedValue: "2022-09-06 03:19:24.942",
		},
		{
			name:          "date",
			colVal:        birthdayDateExt,
			colKind:       typing.Column{KindDetails: dateKind},
			expectedValue: "2022-09-06",
		},
		{
			name:          "time",
			colVal:        birthdayTimeExt,
			colKind:       typing.Column{KindDetails: timeKind},
			expectedValue: "03:19:24",
		},
	}

	for _, testCase := range testCases {
		actualString, actualErr := CastColVal(testCase.colVal, testCase.colKind)
		assert.Equal(t, testCase.expectedErr, actualErr, testCase.name)
		assert.Equal(t, testCase.expectedValue, actualString, testCase.name)
	}
}
