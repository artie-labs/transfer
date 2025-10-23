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

func TestGetTypeFromMap(t *testing.T) {
	{
		// String
		{
			// Not valid (key doesn't exist)
			{
				// Key does not exist
				object := map[string]any{"abc": 123}
				_, err := GetTypeFromMap[string](object, "foo")
				assert.ErrorContains(t, err, `key: "foo" does not exist in object`)
			}
			{
				// nil map
				_, err := GetTypeFromMap[string](nil, "foo")
				assert.ErrorContains(t, err, `key: "foo" does not exist in object`)
			}
			{
				// Not type string
				object := map[string]any{"abc": 123}
				_, err := GetTypeFromMap[string](object, "abc")
				assert.ErrorContains(t, err, "expected type string, got int")
			}
		}
		{
			// Valid
			object := map[string]any{"abc": "123"}
			value, err := GetTypeFromMap[string](object, "abc")
			assert.NoError(t, err)
			assert.Equal(t, "123", value)
		}
	}
	{
		// Boolean
		{
			// Not valid (key does not exist)
			object := map[string]any{"def": true}
			_, err := GetTypeFromMap[bool](object, "foo")
			assert.ErrorContains(t, err, `key: "foo" does not exist in object`)
		}
		{
			// Not valid (wrong type)
			object := map[string]any{"def": 123}
			_, err := GetTypeFromMap[bool](object, "def")
			assert.ErrorContains(t, err, "expected type bool, got int")
		}
		{
			// Valid
			object := map[string]any{"def": true}
			value, err := GetTypeFromMap[bool](object, "def")
			assert.NoError(t, err)
			assert.True(t, value)
		}
	}
}

func TestGetCaseInsensitiveValue(t *testing.T) {
	testCases := []struct {
		name          string
		m             map[string]any
		key           string
		expectedValue any
		expectedFound bool
	}{
		{
			name:          "exact match",
			m:             map[string]any{"foo": "bar", "baz": 123},
			key:           "foo",
			expectedValue: "bar",
			expectedFound: true,
		},
		{
			name:          "case insensitive match",
			m:             map[string]any{"Foo": "bar", "BAZ": 123},
			key:           "foo",
			expectedValue: "bar",
			expectedFound: true,
		},
		{
			name:          "case insensitive match with different case",
			m:             map[string]any{"foo": "bar", "BAZ": 123},
			key:           "FOO",
			expectedValue: "bar",
			expectedFound: true,
		},
		{
			name:          "no match",
			m:             map[string]any{"foo": "bar", "baz": 123},
			key:           "qux",
			expectedValue: nil,
			expectedFound: false,
		},
		{
			name:          "empty map",
			m:             map[string]any{},
			key:           "foo",
			expectedValue: nil,
			expectedFound: false,
		},
		{
			name:          "nil map",
			m:             nil,
			key:           "foo",
			expectedValue: nil,
			expectedFound: false,
		},
		{
			name:          "multiple case variations",
			m:             map[string]any{"Created_At": "2023-01-01", "updated_at": "2023-01-02", "DELETED_AT": "2023-01-03"},
			key:           "created_at",
			expectedValue: "2023-01-01",
			expectedFound: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			value, found := GetCaseInsensitiveValue(testCase.m, testCase.key)
			assert.Equal(t, testCase.expectedFound, found)
			assert.Equal(t, testCase.expectedValue, value)
		})
	}
}
