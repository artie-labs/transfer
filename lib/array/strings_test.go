package array

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringsJoinAddSingleQuotes(t *testing.T) {
	foo := []string{
		"abc",
		"def",
		"ggg",
	}

	assert.Equal(t, "'abc','def','ggg'", StringsJoinAddSingleQuotes(foo))
}

func TestToArrayString(t *testing.T) {
	type _testCase struct {
		name          string
		val           any
		recastAsArray bool
		expectedList  []string
		expectedErr   error
	}

	testCases := []_testCase{
		{
			name: "nil",
		},
		{
			name:         "wrong data type",
			val:          true,
			expectedList: nil,
			expectedErr:  fmt.Errorf("wrong data type, kind: bool"),
		},
		{
			name:         "list of numbers",
			val:          []int{1, 2, 3, 4, 5},
			expectedList: []string{"1", "2", "3", "4", "5"},
		},
		{
			name:         "list of strings",
			val:          []string{"abc", "def", "ghi"},
			expectedList: []string{"abc", "def", "ghi"},
		},
		{
			name:         "list of bools",
			val:          []bool{true, false, true},
			expectedList: []string{"true", "false", "true"},
		},
		{
			name: "array of nested objects",
			val: []map[string]any{
				{
					"foo": "bar",
				},
				{
					"hello": "world",
				},
			},
			expectedList: []string{`{"foo":"bar"}`, `{"hello":"world"}`},
		},
		{
			name: "array of nested lists",
			val: [][]string{
				{
					"foo", "bar",
				},
				{
					"abc", "def",
				},
			},
			expectedList: []string{"[foo bar]", "[abc def]"},
		},
		{
			name:          "boolean, but recasting as an array",
			val:           true,
			expectedList:  []string{"true"},
			recastAsArray: true,
		},
	}

	for _, testCase := range testCases {
		actualString, actualErr := InterfaceToArrayString(testCase.val, testCase.recastAsArray)
		assert.Equal(t, testCase.expectedList, actualString, testCase.name)
		assert.Equal(t, testCase.expectedErr, actualErr, testCase.name)
	}

}

func TestNotEmpty(t *testing.T) {
	notEmptyList := []string{
		"aaa",
		"foo",
		"bar",
	}

	assert.False(t, Empty(notEmptyList))

	notEmptyList = append(notEmptyList, "")
	assert.True(t, Empty(notEmptyList))
}
