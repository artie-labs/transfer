package typing

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ParseValue(t *testing.T) {
	{
		// Optional schema exists, so we are using it
		optionalSchema := map[string]KindDetails{"created_at": String}
		for _, val := range []any{"2023-01-01", nil} {
			assert.Equal(t, String, ParseValue("created_at", optionalSchema, val))
		}
	}
	{
		// Invalid
		assert.Equal(t, ParseValue("", nil, nil), Invalid)
		assert.Equal(t, ParseValue("", nil, errors.New("hello")), Invalid)
	}
	{
		// Nil
		assert.Equal(t, ParseValue("", nil, ""), String)
		assert.Equal(t, ParseValue("", nil, "nil"), String)
		assert.Equal(t, ParseValue("", nil, nil), Invalid)
	}
	{
		// Floats
		assert.Equal(t, ParseValue("", nil, 7.5), Float)
		assert.Equal(t, ParseValue("", nil, -7.4999999), Float)
		assert.Equal(t, ParseValue("", nil, 7.0), Float)
	}
	{
		// Integers
		assert.Equal(t, ParseValue("", nil, 9), Integer)
		assert.Equal(t, ParseValue("", nil, math.MaxInt), Integer)
		assert.Equal(t, ParseValue("", nil, -1*math.MaxInt), Integer)
	}
	{
		// Boolean
		assert.Equal(t, ParseValue("", nil, true), Boolean)
		assert.Equal(t, ParseValue("", nil, false), Boolean)
	}
	{
		// Strings
		possibleStrings := []string{
			"dusty",
			"robin",
			"abc",
		}

		for _, possibleString := range possibleStrings {
			assert.Equal(t, ParseValue("", nil, possibleString), String)
		}
	}
	{
		// Arrays
		assert.Equal(t, ParseValue("", nil, []string{"a", "b", "c"}), Array)
		assert.Equal(t, ParseValue("", nil, []any{"a", 123, "c"}), Array)
		assert.Equal(t, ParseValue("", nil, []int64{1}), Array)
		assert.Equal(t, ParseValue("", nil, []bool{false}), Array)
		assert.Equal(t, ParseValue("", nil, []any{false, true}), Array)
	}
	{
		// Time in string w/ no schema
		kindDetails := ParseValue("", nil, "00:18:11.13116+00")
		assert.Equal(t, String, kindDetails)
	}
	{
		// Maps
		randomMaps := []any{
			map[string]any{
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
			map[string]any{
				"food": map[string]any{
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
}
