package array

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestStringsJoinAddPrefix(t *testing.T) {
	foo := []string{
		"abc",
		"def",
		"ggg",
	}

	args := StringsJoinAddPrefixArgs{
		Vals:      foo,
		Separator: ", ",
		Prefix:    "ARTIE",
	}

	assert.Equal(t, StringsJoinAddPrefix(args), "ARTIEabc, ARTIEdef, ARTIEggg")
}

func TestStringsJoinAddPrefix_ToastedColumns(t *testing.T) {
	toastedCols := []string{
		"toast_test",
		"toast_test_2",
	}

	args := StringsJoinAddPrefixArgs{
		Vals:      toastedCols,
		Separator: " AND ",
		Prefix:    "cc.",
		Suffix:    fmt.Sprintf("!='%s'", constants.ToastUnavailableValuePlaceholder),
	}

	assert.Equal(t, StringsJoinAddPrefix(args), "cc.toast_test!='__debezium_unavailable_value' AND cc.toast_test_2!='__debezium_unavailable_value'")
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
