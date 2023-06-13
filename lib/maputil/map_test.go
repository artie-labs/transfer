package maputil

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetKeyFromMap(t *testing.T) {
	var obj map[string]interface{}
	val := GetKeyFromMap(obj, "invalid", "dusty the mini aussie")
	assert.Equal(t, val, "dusty the mini aussie")

	obj = make(map[string]interface{})
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
	object := map[string]interface{}{
		"abc":          "123",
		"abc (number)": 123,
		"def":          true,
		"ghi":          "hello",
		"123":          "-321",
		"123 (number)": -321,
	}

	type _testCase struct {
		name          string
		obj           map[string]interface{}
		key           string
		expectedValue int
		expectError   bool
	}

	testCases := []_testCase{
		{
			name:          "happy path",
			obj:           object,
			key:           "abc",
			expectedValue: 123,
		},
	}

	for _, testCase := range testCases {
		value, err := GetIntegerFromMap(testCase.obj, testCase.key)
		if testCase.expectError {
			assert.Error(t, err, testCase.name)
		} else {
			assert.Equal(t, reflect.Int, reflect.TypeOf(value).Kind())
			assert.Equal(t, testCase.expectedValue, value)
			assert.NoError(t, err, testCase.name)
		}
	}
}
