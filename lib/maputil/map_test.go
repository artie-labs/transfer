package maputil

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetKeyFromMap(t *testing.T) {
	var obj map[string]any
	val := GetKeyFromMap(obj, "invalid", "dusty the mini aussie")
	assert.Equal(t, val, "dusty the mini aussie")

	obj = make(map[string]any)
	val = GetKeyFromMap(obj, "invalid", "dusty the mini aussie")
	assert.Equal(t, val, "dusty the mini aussie")

	obj["foo"] = "bar"
	val = GetKeyFromMap(obj, "foo", "robin")
	assert.Equal(t, val, "bar")

	val = GetKeyFromMap(obj, "foo#1", "robin")
	assert.Equal(t, val, "robin")

	val = GetKeyFromMap(nil, "foo#1", "robin55")
	assert.Equal(t, val, "robin55")
}

func TestGetInt32FromMap(t *testing.T) {
	object := map[string]any{
		"abc":          "123",
		"abc (number)": 123,
		"def":          true,
		"ghi":          "hello",
		"123":          "-321",
		"123 (number)": -321,
		"maxInt32":     math.MaxInt32,
		"int64":        math.MaxInt32 + 1,
	}

	testCases := []struct {
		name          string
		obj           map[string]any
		key           string
		expectedValue int32
		expectedErr   string
	}{
		{
			name:          "happy path with string value",
			obj:           object,
			key:           "abc",
			expectedValue: 123,
		},
		{
			name:          "happy path with number value",
			obj:           object,
			key:           "abc (number)",
			expectedValue: 123,
		},
		{
			name:        "non-existing key",
			obj:         object,
			key:         "xyz",
			expectedErr: "key: xyz does not exist in object",
		},
		{
			name:        "boolean value",
			obj:         object,
			key:         "def",
			expectedErr: "key: def is not type integer",
		},
		{
			name:        "non-numeric string value",
			obj:         object,
			key:         "ghi",
			expectedErr: "key: ghi is not type integer",
		},
		{
			name:          "negative number as string",
			obj:           object,
			key:           "123",
			expectedValue: -321,
		},
		{
			name:          "negative number",
			obj:           object,
			key:           "123 (number)",
			expectedValue: -321,
		},
		{
			name:          "max int32",
			obj:           object,
			key:           "maxInt32",
			expectedValue: int32(math.MaxInt32),
		},
		{
			name:        "int64",
			obj:         object,
			key:         "int64",
			expectedErr: "value out of range",
		},
	}

	for _, testCase := range testCases {
		value, err := GetInt32FromMap(testCase.obj, testCase.key)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		} else {
			assert.Equal(t, reflect.Int32, reflect.TypeOf(value).Kind())
			assert.Equal(t, testCase.expectedValue, value)
			assert.NoError(t, err, testCase.name)
		}
	}
}

func TestGetStringFromMap(t *testing.T) {
	{
		// Not valid (key doesn't exist)
		{
			// Key does not exist
			object := map[string]any{"abc": 123}
			_, err := GetStringFromMap(object, "foo")
			assert.ErrorContains(t, err, `key: "foo" does not exist in object`)
		}
		{
			// nil map
			_, err := GetStringFromMap(nil, "foo")
			assert.ErrorContains(t, err, `key: "foo" does not exist in object`)
		}
		{
			// Not type string
			object := map[string]any{"abc": 123}
			_, err := GetStringFromMap(object, "abc")
			assert.ErrorContains(t, err, "expected type string, got int")
		}
	}
	{
		// Valid
		object := map[string]any{"abc": "123"}
		value, err := GetStringFromMap(object, "abc")
		assert.NoError(t, err)
		assert.Equal(t, "123", value)
	}

}
