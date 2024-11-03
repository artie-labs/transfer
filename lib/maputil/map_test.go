package maputil

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetKeyFromMap(t *testing.T) {
	{
		// nil map, should return default value
		val := GetKeyFromMap(nil, "invalid", "dusty the mini aussie")
		assert.Equal(t, val, "dusty the mini aussie")
	}
	{
		// empty map, should return default value
		val := GetKeyFromMap(map[string]any{}, "invalid", "dusty the mini aussie")
		assert.Equal(t, val, "dusty the mini aussie")
	}
	{
		// key exists
		obj := map[string]any{"foo": "bar"}
		val := GetKeyFromMap(obj, "foo", "dusty")
		assert.Equal(t, val, "bar")
	}
	{
		// key doesn't exist, should return default value
		obj := map[string]any{"foo": "bar"}
		val := GetKeyFromMap(obj, "foo#1", "dusty")
		assert.Equal(t, val, "dusty")
	}
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

func TestGetTypeFromMapWithDefault(t *testing.T) {
	{
		// nil map, should return default value
		val, err := GetTypeFromMapWithDefault[string](nil, "invalid", "dusty the mini aussie")
		assert.Equal(t, val, "dusty the mini aussie")
		assert.NoError(t, err)
	}
	{
		// map is not empty, but key does not exist
		obj := map[string]any{"foo": "bar"}
		val, err := GetTypeFromMapWithDefault[string](obj, "foo#1", "dusty")
		assert.Equal(t, val, "dusty")
		assert.NoError(t, err)
	}
	{
		// key exists, but not type string
		obj := map[string]any{"foo": 123}
		val, err := GetTypeFromMapWithDefault[string](obj, "foo", "dusty")
		assert.Equal(t, val, "")
		assert.ErrorContains(t, err, `expected key "foo" to be type string, got int`)
	}
	{
		// key exists, type string
		obj := map[string]any{"foo": "bar"}
		val, err := GetTypeFromMapWithDefault[string](obj, "foo", "dusty")
		assert.Equal(t, val, "bar")
		assert.NoError(t, err)
	}
}
