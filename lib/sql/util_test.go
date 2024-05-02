package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteLiteral(t *testing.T) {
	testCases := []struct {
		name     string
		colVal   string
		expected string
	}{
		{
			name:     "string",
			colVal:   "hello",
			expected: "'hello'",
		},
		{
			name:     "string that requires escaping",
			colVal:   "bobby o'reilly",
			expected: `'bobby o\'reilly'`,
		},
		{
			name:     "string with line breaks",
			colVal:   "line1 \n line 2",
			expected: "'line1 \n line 2'",
		},
		{
			name:     "string with existing backslash",
			colVal:   `hello \ there \ hh`,
			expected: `'hello \\ there \\ hh'`,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expected, QuoteLiteral(testCase.colVal), testCase.name)
	}
}
