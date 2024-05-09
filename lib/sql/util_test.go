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

func TestQuoteIdentifiers(t *testing.T) {
	assert.Equal(t, []string{}, QuoteIdentifiers([]string{}, BigQueryDialect{}))
	assert.Equal(t, []string{"`a`", "`b`", "`c`"}, QuoteIdentifiers([]string{"a", "b", "c"}, BigQueryDialect{}))
}

func TestParseDataTypeDefinition(t *testing.T) {
	{
		dataType, args, err := ParseDataTypeDefinition("number")
		assert.NoError(t, err)
		assert.Equal(t, "number", dataType)
		assert.Empty(t, args)
	}
	{
		dataType, args, err := ParseDataTypeDefinition("number(5,2)")
		assert.NoError(t, err)
		assert.Equal(t, "number", dataType)
		assert.Equal(t, []string{"5", "2"}, args)
	}
	{
		dataType, args, err := ParseDataTypeDefinition("number(5, 2)")
		assert.NoError(t, err)
		assert.Equal(t, "number", dataType)
		assert.Equal(t, []string{"5", "2"}, args)
	}
	{
		dataType, args, err := ParseDataTypeDefinition("VARCHAR(1234)")
		assert.NoError(t, err)
		assert.Equal(t, "VARCHAR", dataType)
		assert.Equal(t, []string{"1234"}, args)
	}
	{
		// Spaces:
		dataType, args, err := ParseDataTypeDefinition("   VARCHAR   (1234)  ")
		assert.NoError(t, err)
		assert.Equal(t, "VARCHAR", dataType)
		assert.Equal(t, []string{"1234"}, args)
	}
	{
		// Malformed args:
		_, _, err := ParseDataTypeDefinition("VARCHAR(1234")
		assert.ErrorContains(t, err, "missing closing parenthesis")
	}
}
