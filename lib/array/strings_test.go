package array

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringsJoinAddPrefix(t *testing.T) {
	foo := []string{
		"abc",
		"def",
		"ggg",
	}

	assert.Equal(t, StringsJoinAddPrefix(foo, ", ", "ARTIE"), "ARTIEabc, ARTIEdef, ARTIEggg")
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

func TestRemoveElement(t *testing.T) {
	type testCaseScenario struct {
		name            string
		list            []string
		elementToRemove string
		expectedList    []string
	}

	testCases := []testCaseScenario{
		{
			name:            "did not remove anything",
			list:            []string{"a", "b", "c"},
			elementToRemove: "d",
			expectedList:    []string{"a", "b", "c"},
		},
		{
			name:            "removed first element",
			list:            []string{"a", "b", "c"},
			elementToRemove: "a",
			expectedList:    []string{"b", "c"},
		},
		{
			name:            "removed 2nd element",
			list:            []string{"a", "b", "c"},
			elementToRemove: "b",
			expectedList:    []string{"a", "c"},
		},
		{
			name:            "removed last element",
			list:            []string{"a", "b", "c"},
			elementToRemove: "c",
			expectedList:    []string{"a", "b"},
		},
	}

	for _, testCase := range testCases {
		newList := RemoveElement(testCase.list, testCase.elementToRemove)
		assert.Equal(t, testCase.expectedList, newList, testCase.name)
	}

}
