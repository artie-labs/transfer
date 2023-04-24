package typing

import (
	"errors"
	"fmt"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
	"math"
	"strings"
	"testing"
)

func TestJSONString(t *testing.T) {
	assert.Equal(t, true, IsJSON(`{"hello": "world"}`))
	assert.Equal(t, true, IsJSON(`{}`))
	assert.Equal(t, true, IsJSON(strings.TrimSpace(`
	{
		"hello": {
			"world": {
				"nested_value": true
			}
		},
		"add_a_list_here": [1, 2, 3, 4],
		"number": 7.5,
		"integerNum": 7
	}
	`)))

	assert.Equal(t, false, IsJSON(`{null`))
	assert.Equal(t, false, IsJSON(`{null}`))
	assert.Equal(t, false, IsJSON(`{abc: def}`))
}

func TestParseValueBasic(t *testing.T) {
	// Floats
	assert.Equal(t, ParseValue("", nil, 7.5), Float)
	assert.Equal(t, ParseValue("", nil, -7.4999999), Float)
	assert.Equal(t, ParseValue("", nil, 7.0), Float)

	// Integers
	assert.Equal(t, ParseValue("", nil, 9), Integer)
	assert.Equal(t, ParseValue("", nil, math.MaxInt), Integer)
	assert.Equal(t, ParseValue("", nil, -1*math.MaxInt), Integer)

	// Invalid
	assert.Equal(t, ParseValue("", nil, nil), Invalid)
	assert.Equal(t, ParseValue("", nil, errors.New("hello")), Invalid)

	// Boolean
	assert.Equal(t, ParseValue("", nil, true), Boolean)
	assert.Equal(t, ParseValue("", nil, false), Boolean)
}

func TestParseValueArrays(t *testing.T) {
	assert.Equal(t, ParseValue("", nil, []string{"a", "b", "c"}), Array)
	assert.Equal(t, ParseValue("", nil, []interface{}{"a", 123, "c"}), Array)
	assert.Equal(t, ParseValue("", nil, []int64{1}), Array)
	assert.Equal(t, ParseValue("", nil, []bool{false}), Array)
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
		assert.Equal(t, ParseValue("", nil, randomMap), Struct, fmt.Sprintf("Failed message is: %v", randomMap))
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
		assert.Equal(t, ParseValue("", nil, possibleDate).ExtendedTimeDetails.Type, ext.DateTime.Type, fmt.Sprintf("Failed format, value is: %v", possibleDate))

		// Test the parseDT function as well.
		ts, err := ext.ParseExtendedDateTime(fmt.Sprint(possibleDate))
		fmt.Println("possibleDate", possibleDate, "ts", ts)
		assert.NoError(t, err, err)
		assert.False(t, ts.IsZero(), ts)
	}

	ts, err := ext.ParseExtendedDateTime("random")
	assert.Error(t, err, err)
	assert.Nil(t, ts)
}

func TestTime(t *testing.T) {
	kindDetails := ParseValue("", nil, "00:18:11.13116+00")
	// 00:42:26.693631Z
	assert.Equal(t, ETime.Kind, kindDetails.Kind)
	assert.Equal(t, ext.TimeKindType, kindDetails.ExtendedTimeDetails.Type)
}

func TestString(t *testing.T) {
	possibleStrings := []string{
		"dusty",
		"robin",
		"abc",
	}

	for _, possibleString := range possibleStrings {
		assert.Equal(t, ParseValue("", nil, possibleString), String)
	}
}

func TestOptionalSchema(t *testing.T) {
	kd := ParseValue("", nil, true)
	assert.Equal(t, kd, Boolean)

	// Key in a nil-schema
	kd = ParseValue("key", nil, true)
	assert.Equal(t, kd, Boolean)

	// Non-existent key in the schema.
	optionalSchema := map[string]KindDetails{
		"created_at": String,
	}

	// Parse it as a date since it doesn't exist in the optional schema.
	kd = ParseValue("updated_at", optionalSchema, "2023-01-01")
	assert.Equal(t, ext.Date.Type, kd.ExtendedTimeDetails.Type)

	// Respecting the optional schema
	kd = ParseValue("created_at", optionalSchema, "2023-01-01")
	assert.Equal(t, String, kd)
}
