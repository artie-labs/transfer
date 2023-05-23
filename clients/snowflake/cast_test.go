package snowflake

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

type _testCase struct {
	name    string
	colVal  interface{}
	colKind typing.Column

	expectedString string
	expectErr      bool
}

func evaluateTestCase(t *testing.T, testCase _testCase) {
	actualString, actualErr := CastColValStaging(testCase.colVal, testCase.colKind)
	if testCase.expectErr {
		assert.Error(t, actualErr, testCase.name)
	}

	assert.Equal(t, testCase.expectedString, actualString, testCase.name)
}

func (s *SnowflakeTestSuite) TestCastColValStaging_Basic() {
	testCases := []_testCase{
		{
			name:   "string",
			colVal: "foo",
			colKind: typing.Column{
				KindDetails: typing.String,
			},

			expectedString: "foo",
		},
		{
			name:   "integer",
			colVal: 7,
			colKind: typing.Column{
				KindDetails: typing.Integer,
			},
			expectedString: "7",
		},
		{
			name:   "boolean",
			colVal: true,
			colKind: typing.Column{
				KindDetails: typing.Boolean,
			},
			expectedString: "true",
		},
		{
			name:   "array",
			colVal: []string{"hello", "there"},
			colKind: typing.Column{
				KindDetails: typing.Array,
			},
			expectedString: `["hello","there"]`,
		},
		{
			name:   "JSON string",
			colVal: `{"hello": "world"}`,
			colKind: typing.Column{
				KindDetails: typing.Struct,
			},
			expectedString: `{"hello": "world"}`,
		},
	}

	for _, testCase := range testCases {
		evaluateTestCase(s.T(), testCase)
	}
}
