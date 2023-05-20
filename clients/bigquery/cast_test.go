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

		expectedErr    error
		expectedString string
	}

	testCases := []_testCase{
		{
			name:           "escaping string",
			colVal:         "foo",
			colKind:        typing.Column{KindDetails: typing.String},
			expectedString: "foo",
		},
		{
			name:           "123 as int",
			colVal:         123,
			colKind:        typing.Column{KindDetails: typing.Integer},
			expectedString: "123",
		},
		{
			name:           "struct",
			colVal:         `{"hello": "world"}`,
			colKind:        typing.Column{KindDetails: typing.Struct},
			expectedString: `{"hello": "world"}`,
		},
		{
			name:           "array",
			colVal:         []int{1, 2, 3, 4, 5},
			colKind:        typing.Column{KindDetails: typing.Array},
			expectedString: `['1','2','3','4','5']`,
		},
	}

	for _, testCase := range testCases {
		actualString, actualErr := CastColVal(testCase.colVal, testCase.colKind)
		assert.Equal(t, testCase.expectedErr, actualErr, testCase.name)
		assert.Equal(t, testCase.expectedString, *actualString, testCase.name)
	}
}
