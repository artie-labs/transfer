package typing

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestParseValueBasic(t *testing.T) {
	// Floats
	assert.Equal(t, ParseValue(7.5), Float)
	assert.Equal(t, ParseValue(-7.4999999), Float)
	assert.Equal(t, ParseValue(7.0), Float)

	// Integers
	assert.Equal(t, ParseValue(9), Integer)
	assert.Equal(t, ParseValue(math.MaxInt), Integer)
	assert.Equal(t, ParseValue(-1*math.MaxInt), Integer)

	// Invalid
	assert.Equal(t, ParseValue(nil), Invalid)
	assert.Equal(t, ParseValue(errors.New("hello")), Invalid)

	// Boolean
	assert.Equal(t, ParseValue(true), Boolean)
	assert.Equal(t, ParseValue(false), Boolean)
}

func TestParseValueArrays(t *testing.T) {
	assert.Equal(t, ParseValue([]string{"a", "b", "c"}), Array)
	assert.Equal(t, ParseValue([]interface{}{"a", 123, "c"}), Array)
	assert.Equal(t, ParseValue([]int32{1}), Array)
	assert.Equal(t, ParseValue([]bool{false}), Array)
}

func TestParseValueMaps(t *testing.T) {
	randomMaps := []interface{}{
		map[string]interface{}{
			"foo":   "bar",
			"dog":   "dusty",
			"breed": "australian shepherd",
		},
		map[string]bool{
			"foo": true,
			"bar": false,
		},
		map[int]int{
			1: 1,
			2: 2,
			3: 3,
		},
		map[string]interface{}{
			"food": map[string]interface{}{
				"pizza": "slice",
				"fruit": "apple",
			},
			"music": []string{"a", "b", "c"},
		},
	}

	for _, randomMap := range randomMaps {
		assert.Equal(t, ParseValue(randomMap), Struct, fmt.Sprintf("Failed message is: %v", randomMap))
	}
}

func TestDateTime(t *testing.T) {
	// Took this list from the Go time library.
	possibleDates := []interface{}{
		"01/02 03:04:05PM '06 -0700", // The reference time, in numerical order.
		"Mon Jan 2 15:04:05 2006",
		"Mon Jan 2 15:04:05 MST 2006",
		"Mon Jan 02 15:04:05 -0700 2006",
		"02 Jan 06 15:04 MST",
		"02 Jan 06 15:04 -0700", // RFC822 with numeric zone
		"Monday, 02-Jan-06 15:04:05 MST",
		"Mon, 02 Jan 2006 15:04:05 MST",
		"Mon, 02 Jan 2006 15:04:05 -0700", // RFC1123 with numeric zone
		"2019-10-12T14:20:50.52+07:00",
	}

	for _, possibleDate := range possibleDates {
		assert.Equal(t, ParseValue(possibleDate), DateTime, fmt.Sprintf("Failed format, value is: %v", possibleDate))
	}
}

func TestString(t *testing.T) {
	possibleStrings := []string{
		"dusty",
		"robin",
		"abc",
	}

	for _, possibleString := range possibleStrings {
		assert.Equal(t, ParseValue(possibleString), String)
	}
}

func TestEscapeString(t *testing.T) {
	val := "Robin O'Smith"
	escapedString := EscapeString(val)

	fmt.Println("escapedString", escapedString)
	assert.Equal(t, escapedString, val)
}
