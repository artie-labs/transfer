package typing

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing/ext"
)

func Test_ParseValue(t *testing.T) {
	{
		// Invalid
		{
			// Unknown data type
			type Dusty struct{}
			var dusty Dusty
			_, err := ParseValue("dusty", nil, dusty)
			assert.ErrorContains(t, err, "unknown type: typing.Dusty, value: {}")
		}
		{
			// Another unknown data type
			_, err := ParseValue("", nil, fmt.Errorf("hello there"))
			assert.ErrorContains(t, err, "unknown type: *errors.errorString, value: hello there")
		}
	}
	{
		// Optional schema exists, so we are using it
		optionalSchema := map[string]KindDetails{"created_at": String}
		for _, val := range []any{"2023-01-01", nil} {
			assert.Equal(t, String, MustParseValue("created_at", optionalSchema, val))
		}
	}
	{
		// Nil
		assert.Equal(t, MustParseValue("", nil, ""), String)
		assert.Equal(t, MustParseValue("", nil, "nil"), String)
		assert.Equal(t, MustParseValue("", nil, nil), Invalid)
	}
	{
		// Floats
		assert.Equal(t, MustParseValue("", nil, 7.5), Float)
		assert.Equal(t, MustParseValue("", nil, -7.4999999), Float)
		assert.Equal(t, MustParseValue("", nil, 7.0), Float)
	}
	{
		// Integers
		assert.Equal(t, MustParseValue("", nil, 9), Integer)
		assert.Equal(t, MustParseValue("", nil, math.MaxInt), Integer)
		assert.Equal(t, MustParseValue("", nil, -1*math.MaxInt), Integer)
	}
	{
		// json.Number
		for _, variant := range []json.Number{"42", "9223372036854775806", "-100", "3.14", "1e10", "2.5E3"} {
			assert.Equal(t, Float, MustParseValue("", nil, variant))
		}
	}
	{
		// Boolean
		assert.Equal(t, MustParseValue("", nil, true), Boolean)
		assert.Equal(t, MustParseValue("", nil, false), Boolean)
	}
	{
		// Strings
		possibleStrings := []string{
			"dusty",
			"robin",
			"abc",
		}

		for _, possibleString := range possibleStrings {
			assert.Equal(t, MustParseValue("", nil, possibleString), String)
		}
	}
	{
		// Arrays
		assert.Equal(t, MustParseValue("", nil, []string{"a", "b", "c"}), Array)
		assert.Equal(t, MustParseValue("", nil, []any{"a", 123, "c"}), Array)
		assert.Equal(t, MustParseValue("", nil, []int64{1}), Array)
		assert.Equal(t, MustParseValue("", nil, []bool{false}), Array)
		assert.Equal(t, MustParseValue("", nil, []any{false, true}), Array)
	}
	{
		// Time in string w/ no schema
		kindDetails := MustParseValue("", nil, "00:18:11.13116+00")
		assert.Equal(t, String, kindDetails)
	}
	{
		// time.Time returns TimestampTZ
		assert.Equal(t, TimestampTZ, MustParseValue("", nil, time.Now()))
	}
	{
		// ext.Time returns TimeKindDetails
		assert.Equal(t, TimeKindDetails, MustParseValue("", nil, ext.NewTime(time.Now())))
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
			assert.Equal(t, MustParseValue("", nil, randomMap), Struct, fmt.Sprintf("Failed message is: %v", randomMap))
		}
	}
}
