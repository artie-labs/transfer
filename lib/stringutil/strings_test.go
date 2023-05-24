package stringutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrap(t *testing.T) {
	type _testCase struct {
		name           string
		colVal         interface{}
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
			name:           "string that requires escaping",
			colVal:         "bobby o'reilly",
			expectedString: `'bobby o\'reilly'`,
		},
		{
			name:           "string that requires escaping (no quotes)",
			colVal:         "bobby o'reilly",
			expectedString: `bobby o\'reilly`,
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

func TestReverse(t *testing.T) {
	val := "hello"
	assert.Equal(t, Reverse(val), "olleh")

	assert.Equal(t, Reverse("alone"), "enola")

	val = "foo12345k321k3okldsadsa"
	assert.Equal(t, Reverse(val), Reverse(Reverse(Reverse(val))))
	assert.Equal(t, val, Reverse(Reverse(val)))
}

func TestEmpty(t *testing.T) {
	assert.False(t, Empty("hi", "there", "artie", "transfer"))
	assert.False(t, Empty("dusty"))

	assert.True(t, Empty("robin", "jacqueline", "charlie", ""))
	assert.True(t, Empty(""))
}

func TestEscapeSpaces(t *testing.T) {
	colsToExpectation := map[string]map[string]interface{}{
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

func TestLineBreaksToCarriageReturns(t *testing.T) {
	paragraph := `Dog
walked
over
the
hill
`
	text := LineBreaksToCarriageReturns(paragraph)
	assert.Equal(t, `Dog\nwalked\nover\nthe\nhill\n`, text, paragraph)

	nonParagraphs := []string{
		"foo", "思翰", "Gene Capron", "aba4195bde80192dff98f2cab0ecdca954482826",
		"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAABGdBTUEAAK/INwWK6QAAABl0RVh0U29mdHdhcmUAQWRvYmUgSW1hZ2VSZWFkeXHJZTwAAAFzSURBVDjLY/j//z8DPlxYWFgAxA9ANDZ5BiIMeASlH5BswPz58+uampo2kuUCkGYgPg/EQvgsweZk5rlz5zYSoxnDAKBmprq6umONjY1vsmdeamvd9Pzc1N2vv/Zse/k0a/6jZWGT7hWGTLhrEdR7hwOrAfPmzWtob29/XlRc9qdjw8P76fMeTU2c9WBi5LQH7UB6ftS0B9MDe+7k+XfeCvRpu6Xr1XJTEMPP2TMvlkzZ8fhn9JSb+ujO9e+6ZebbcSvMu/Wmm2fzDSv3hmuGsHh+BAptkJ9Llj3e2LDu2SVcfvZqucHm0XhD163+mplLzVVtjHgGar7asO75bUKB51R9Vdih4ooqRkprXPfsXsfm558JGQDCtqWXmDAEi5Y+PjNhx4v/QL8aE2MIhkD8zAcbJ+189d+z5UYOWQZ4t9xsnLjj5f/A3ltLyDIAGDXe7Zue/89b/OiZY8UVNpINAEaNUOWqp38qVj3+DwykQEIGAABS5b0Ghvs3EQAAAABJRU5ErkJggg==",
		"64529513d746ff455a986505", "gcapron6x@gizmodo.com",
	}

	for _, nonParagraph := range nonParagraphs {
		assert.Equal(t, nonParagraph, LineBreaksToCarriageReturns(nonParagraph))
	}
}
