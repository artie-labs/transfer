package maputil

import (
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

func TestGetIntegerFromMap(t *testing.T) {
	object := map[string]any{
		"abc":          "123",
		"abc (number)": 123,
		"def":          true,
		"ghi":          "hello",
		"123":          "-321",
		"123 (number)": -321,
	}

	testCases := []struct {
		name          string
		obj           map[string]any
		key           string
		expectedValue int
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
			expectedErr: "true",
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
	}

	for _, testCase := range testCases {
		value, err := GetIntegerFromMap(testCase.obj, testCase.key)
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, err, testCase.expectedErr, testCase.name)
		} else {
			assert.Equal(t, reflect.Int, reflect.TypeOf(value).Kind())
			assert.Equal(t, testCase.expectedValue, value)
			assert.NoError(t, err, testCase.name)
		}
	}
}
