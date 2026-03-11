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

func TestReplaceInvalidUTF8(t *testing.T) {
	{
		// Already valid UTF-8 is returned as-is.
		assert.Equal(t, "hello world", ReplaceInvalidUTF8("hello world"))
		assert.Equal(t, "Spesen März 17", ReplaceInvalidUTF8("Spesen März 17"))
		assert.Equal(t, "", ReplaceInvalidUTF8(""))
	}
	{
		// Latin-1 0xE4 = 'ä' (U+00E4) should be re-encoded as valid UTF-8.
		latin1 := "Spesen M\xe4rz 17"
		result := ReplaceInvalidUTF8(latin1)
		assert.Equal(t, "Spesen März 17", result)
	}
	{
		// Multiple invalid bytes in a row.
		latin1 := "\xfc\xf6\xe4" // üöä in Latin-1
		result := ReplaceInvalidUTF8(latin1)
		assert.Equal(t, "üöä", result)
	}
	{
		// Mix of valid UTF-8 and invalid bytes.
		mixed := "hello \xe9 world" // \xe9 = 'é' in Latin-1
		result := ReplaceInvalidUTF8(mixed)
		assert.Equal(t, "hello é world", result)
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
