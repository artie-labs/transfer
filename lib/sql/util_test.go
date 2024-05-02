package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteLiteral(t *testing.T) {
	type _testCase struct {
		name           string
		colVal         string
		expectedString string
	}

	testCases := []_testCase{
		{
			name:           "string",
			colVal:         "hello",
			expectedString: "'hello'",
		},
		{
			name:           "string that requires escaping",
			colVal:         "bobby o'reilly",
			expectedString: `'bobby o\'reilly'`,
		},
		{
			name:           "string with line breaks",
			colVal:         "line1 \n line 2",
			expectedString: "'line1 \n line 2'",
		},
		{
			name:           "string with existing backslash",
			colVal:         `hello \ there \ hh`,
			expectedString: `'hello \\ there \\ hh'`,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectedString, QuoteLiteral(testCase.colVal), testCase.name)
	}
}
