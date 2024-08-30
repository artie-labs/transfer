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
		// Invalid
		assert.Equal(t, ParseValue(Settings{}, "", nil, nil), Invalid)
		assert.Equal(t, ParseValue(Settings{}, "", nil, errors.New("hello")), Invalid)
	}
	{
		// Nil
		assert.Equal(t, ParseValue(Settings{}, "", nil, ""), String)
		assert.Equal(t, ParseValue(Settings{}, "", nil, "nil"), String)
		assert.Equal(t, ParseValue(Settings{}, "", nil, nil), Invalid)
	}
	{
		// Floats
		assert.Equal(t, ParseValue(Settings{}, "", nil, 7.5), Float)
		assert.Equal(t, ParseValue(Settings{}, "", nil, -7.4999999), Float)
		assert.Equal(t, ParseValue(Settings{}, "", nil, 7.0), Float)
	}
	{
		// Integers
		assert.Equal(t, ParseValue(Settings{}, "", nil, 9), Integer)
		assert.Equal(t, ParseValue(Settings{}, "", nil, math.MaxInt), Integer)
		assert.Equal(t, ParseValue(Settings{}, "", nil, -1*math.MaxInt), Integer)
	}
	{
		// Boolean
		assert.Equal(t, ParseValue(Settings{}, "", nil, true), Boolean)
		assert.Equal(t, ParseValue(Settings{}, "", nil, false), Boolean)
	}
	{
		// Strings
		possibleStrings := []string{
			"dusty",
			"robin",
			"abc",
		}

		for _, possibleString := range possibleStrings {
			assert.Equal(t, ParseValue(Settings{}, "", nil, possibleString), String)
		}
	}
	{
		// Arrays
		assert.Equal(t, ParseValue(Settings{}, "", nil, []string{"a", "b", "c"}), Array)
		assert.Equal(t, ParseValue(Settings{}, "", nil, []any{"a", 123, "c"}), Array)
		assert.Equal(t, ParseValue(Settings{}, "", nil, []int64{1}), Array)
		assert.Equal(t, ParseValue(Settings{}, "", nil, []bool{false}), Array)
		assert.Equal(t, ParseValue(Settings{}, "", nil, []any{false, true}), Array)
	}
	{
		// Time
		kindDetails := ParseValue(Settings{}, "", nil, "00:18:11.13116+00")
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
			assert.Equal(t, ParseValue(Settings{}, "", nil, randomMap), Struct, fmt.Sprintf("Failed message is: %v", randomMap))
		}
	}
}

func TestOptionalSchema(t *testing.T) {
	{
		optionalSchema := map[string]KindDetails{
			"created_at": String,
		}

		// Respect the schema if the value is not null.
		assert.Equal(t, String, ParseValue(Settings{}, "created_at", optionalSchema, "2023-01-01"))
		// Kind is invalid because `createAllColumnsIfAvailable` is not enabled.
		assert.Equal(t, String, ParseValue(Settings{}, "created_at", optionalSchema, nil))
	}
}
