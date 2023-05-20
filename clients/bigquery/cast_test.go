package bigquery

import (
	"testing"

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
			name:          "array",
			colVal:        []int{1, 2, 3, 4, 5},
			colKind:       typing.Column{KindDetails: typing.Array},
			expectedValue: []string{"1", "2", "3", "4", "5"},
		},
	}

	for _, testCase := range testCases {
		actualString, actualErr := CastColVal(testCase.colVal, testCase.colKind)
		assert.Equal(t, testCase.expectedErr, actualErr, testCase.name)
		assert.Equal(t, testCase.expectedValue, actualString, testCase.name)
	}
}
