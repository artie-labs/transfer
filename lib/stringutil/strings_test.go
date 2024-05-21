package stringutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCapitalizeFirstLetter(t *testing.T) {
	{
		assert.Equal(t, "Hello", CapitalizeFirstLetter("hello"))
	}
	{
		assert.Equal(t, "", CapitalizeFirstLetter(""))
	}
	{
		assert.Equal(t, "H", CapitalizeFirstLetter("H"))
	}
}

func TestEscapeBackslashes(t *testing.T) {
	testCases := []struct {
		name           string
		colVal         string
		expectedString string
	}{
		{
			name:           "string",
			colVal:         "hello",
			expectedString: "hello",
		},
		{
			name:           "string",
			colVal:         "bobby o'reilly",
			expectedString: "bobby o'reilly",
		},
		{
			name:           "string with line breaks",
			colVal:         "line1 \n line 2",
			expectedString: "line1 \n line 2",
		},
		{
			name:           "string with existing backslash",
			colVal:         `hello \ there \ hh`,
			expectedString: `hello \\ there \\ hh`,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectedString, EscapeBackslashes(testCase.colVal), testCase.name)
	}
}

func TestEmpty(t *testing.T) {
	assert.False(t, Empty("hi", "there", "artie", "transfer"))
	assert.False(t, Empty("dusty"))

	assert.True(t, Empty("robin", "jacqueline", "charlie", ""))
	assert.True(t, Empty(""))
}

func TestEscapeSpaces(t *testing.T) {
	colsToExpectation := map[string]map[string]any{
		"columnA":  {"escaped": "columnA", "space": false},
		"column_a": {"escaped": "column_a", "space": false},
		"column a": {"escaped": "column__a", "space": true},
	}

	for col, expected := range colsToExpectation {
		containsSpace, escapedString := EscapeSpaces(col)
		assert.Equal(t, expected["escaped"], escapedString)
		assert.Equal(t, expected["space"], containsSpace)
	}
}
