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
	{
		// No escape
		{
			assert.Equal(t, "hello", EscapeBackslashes("hello"))
		}
		{
			// Special char
			assert.Equal(t, `bobby o'reilly`, EscapeBackslashes(`bobby o'reilly`))
		}
		{
			// Line breaks
			assert.Equal(t, "line1 \n line 2", EscapeBackslashes("line1 \n line 2"))
		}
	}
	{
		// Escape
		{
			// Backslash
			assert.Equal(t, `hello \\ there \\ hh`, EscapeBackslashes(`hello \ there \ hh`))
		}
	}
}

func TestEmpty(t *testing.T) {
	{
		// No empty
		assert.False(t, Empty("hi", "there", "artie", "transfer"))
		assert.False(t, Empty("dusty"))
	}
	{
		// Empty
		assert.True(t, Empty("robin", "jacqueline", "charlie", ""))
		assert.True(t, Empty(""))
	}
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
