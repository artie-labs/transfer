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

func TestOverride(t *testing.T) {
	type _testCase struct {
		name        string
		vals        []string
		expectedVal string
	}

	testCases := []_testCase{
		{
			name:        "empty",
			expectedVal: "",
		},
		{
			name:        "empty (empty list)",
			vals:        []string{},
			expectedVal: "",
		},
		{
			name:        "empty (list w/ empty val)",
			vals:        []string{""},
			expectedVal: "",
		},
		{
			name:        "one value",
			vals:        []string{"hi"},
			expectedVal: "hi",
		},
		{
			name:        "override (2 vals)",
			vals:        []string{"hi", "latest"},
			expectedVal: "latest",
		},
		{
			name:        "override (3 vals)",
			vals:        []string{"hi", "", "latest"},
			expectedVal: "latest",
		},
		{
			name:        "override (all empty)",
			vals:        []string{"hii", "", ""},
			expectedVal: "hii",
		},
	}

	for _, testCase := range testCases {
		actualVal := Override(testCase.vals...)
		assert.Equal(t, testCase.expectedVal, actualVal, testCase.name)
	}
}

func TestWrap(t *testing.T) {
	type _testCase struct {
		name           string
		colVal         any
		noQuotes       bool
		expectedString string
	}

	testCases := []_testCase{
		{
			name:           "string",
			colVal:         "hello",
			expectedString: "'hello'",
		},
		{
			name:           "string (no quotes)",
			colVal:         "hello",
			noQuotes:       true,
			expectedString: "hello",
		},
		{
			name:           "string (no quotes)",
			colVal:         "bobby o'reilly",
			noQuotes:       true,
			expectedString: "bobby o'reilly",
		},
		{
			name:           "string that requires escaping",
			colVal:         "bobby o'reilly",
			expectedString: `'bobby o\'reilly'`,
		},
		{
			name:           "string that requires escaping (no quotes)",
			colVal:         "bobby o'reilly",
			expectedString: `bobby o'reilly`,
			noQuotes:       true,
		},
		{
			name:           "string with line breaks",
			colVal:         "line1 \n line 2",
			expectedString: "'line1 \n line 2'",
		},
		{
			name:           "string with line breaks (no quotes)",
			colVal:         "line1 \n line 2",
			expectedString: "line1 \n line 2",
			noQuotes:       true,
		},
		{
			name:           "string with existing backslash",
			colVal:         `hello \ there \ hh`,
			expectedString: `'hello \\ there \\ hh'`,
		},
		{
			name:           "string with existing backslash (no quotes)",
			colVal:         `hello \ there \ hh`,
			expectedString: `hello \\ there \\ hh`,
			noQuotes:       true,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectedString, Wrap(testCase.colVal, testCase.noQuotes), testCase.name)
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
