package array

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestToArrayString(t *testing.T) {
	{
		// Test nil input
		value, err := InterfaceToArrayString(nil, false)
		assert.NoError(t, err)
		var expected []string
		assert.Equal(t, expected, value)
	}
	{
		// Test wrong data type
		_, err := InterfaceToArrayString(true, false)
		assert.ErrorContains(t, err, "wrong data type, kind: bool")
	}
	{
		// Test list of dates
		value, err := InterfaceToArrayString([]time.Time{time.Date(2024, 4, 13, 0, 0, 0, 0, time.UTC), time.Date(2024, 4, 14, 0, 0, 0, 0, time.UTC), time.Date(2024, 4, 15, 0, 0, 0, 0, time.UTC)}, false)
		assert.NoError(t, err)
		assert.Equal(t, []string{"2024-04-13", "2024-04-14", "2024-04-15"}, value)
	}
	{
		// Test list of numbers
		value, err := InterfaceToArrayString([]int{1, 2, 3, 4, 5}, false)
		assert.NoError(t, err)
		assert.Equal(t, []string{"1", "2", "3", "4", "5"}, value)
	}
	{
		// Test list of strings
		value, err := InterfaceToArrayString([]string{"abc", "def", "ghi"}, false)
		assert.NoError(t, err)
		assert.Equal(t, []string{"abc", "def", "ghi"}, value)
	}
	{
		// Test list of booleans
		value, err := InterfaceToArrayString([]bool{true, false, true}, false)
		assert.NoError(t, err)
		assert.Equal(t, []string{"true", "false", "true"}, value)
	}
	{
		// Test array of nested objects
		value, err := InterfaceToArrayString([]map[string]any{{"foo": "bar"}, {"hello": "world"}}, false)
		assert.NoError(t, err)
		assert.Equal(t, []string{`{"foo":"bar"}`, `{"hello":"world"}`}, value)
	}
	{
		// Test array of nested lists
		value, err := InterfaceToArrayString([][]string{
			{
				"foo", "bar",
			},
			{
				"abc", "def",
			},
		}, false)
		assert.NoError(t, err)
		assert.Equal(t, []string{"[foo bar]", "[abc def]"}, value)
	}
	{
		// Test boolean recast as array
		value, err := InterfaceToArrayString(true, true)
		assert.NoError(t, err)
		assert.Equal(t, []string{"true"}, value)
	}
}
