package stringutil

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReverse(t *testing.T) {
	val := "hello"
	assert.Equal(t, Reverse(val), "olleh")

	assert.Equal(t, Reverse("alone"), "enola")
}

func TestReverseComplex(t *testing.T) {
	val := "foo12345k321k3okldsadsa"

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
