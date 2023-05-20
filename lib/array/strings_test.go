package array

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestToArrayString(t *testing.T) {
	type _testCase struct {
		name string
		val  interface{}

		expectedList []string
		expectedErr  error
	}

	testCases := []_testCase{
		{
			name: "nil",
		},
		{
			name:         "wrong data type",
			val:          true,
			expectedList: nil,
			expectedErr:  fmt.Errorf("wrong data type"),
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
			val: []map[string]interface{}{
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
	}

	for _, testCase := range testCases {
		actualString, actualErr := InterfaceToArrayString(testCase.val)
		assert.Equal(t, testCase.expectedList, actualString, testCase.name)
		assert.Equal(t, testCase.expectedErr, actualErr, testCase.name)
	}

}

func TestColumnsUpdateQuery(t *testing.T) {
	type testCase struct {
		name           string
		columns        []string
		columnsToTypes typing.Columns
		expectedString string
		bigQuery       bool
	}

	fooBarCols := []string{"foo", "bar"}

	var (
		happyPathCols      typing.Columns
		stringAndToastCols typing.Columns
		lastCaseColTypes   typing.Columns
	)
	for _, col := range fooBarCols {
		happyPathCols.AddColumn(typing.Column{
			Name:        col,
			KindDetails: typing.String,
			ToastColumn: false,
		})
	}
	for _, col := range fooBarCols {
		var toastCol bool
		if col == "foo" {
			toastCol = true
		}

		stringAndToastCols.AddColumn(typing.Column{
			Name:        col,
			KindDetails: typing.String,
			ToastColumn: toastCol,
		})
	}

	lastCaseCols := []string{"a1", "b2", "c3"}

	for _, lastCaseCol := range lastCaseCols {
		kd := typing.String
		var toast bool
		// a1 - struct + toast, b2 - string + toast, c3 = regular string.
		if lastCaseCol == "a1" {
			kd = typing.Struct
			toast = true
		} else if lastCaseCol == "b2" {
			toast = true
		}

		lastCaseColTypes.AddColumn(typing.Column{
			Name:        lastCaseCol,
			KindDetails: kd,
			ToastColumn: toast,
		})
	}

	testCases := []testCase{
		{
			name:           "happy path",
			columns:        fooBarCols,
			columnsToTypes: happyPathCols,
			expectedString: "foo=cc.foo,bar=cc.bar",
		},
		{
			name:           "string and toast",
			columns:        fooBarCols,
			columnsToTypes: stringAndToastCols,
			expectedString: "foo= CASE WHEN cc.foo != '__debezium_unavailable_value' THEN cc.foo ELSE c.foo END,bar=cc.bar",
		},
		{
			name:           "struct, string and toast string",
			columns:        lastCaseCols,
			columnsToTypes: lastCaseColTypes,
			expectedString: "a1= CASE WHEN cc.a1 != {'key': '__debezium_unavailable_value'} THEN cc.a1 ELSE c.a1 END,b2= CASE WHEN cc.b2 != '__debezium_unavailable_value' THEN cc.b2 ELSE c.b2 END,c3=cc.c3",
		},
		{
			name:           "struct, string and toast string (bigquery)",
			columns:        lastCaseCols,
			columnsToTypes: lastCaseColTypes,
			bigQuery:       true,
			expectedString: `a1= CASE WHEN TO_JSON_STRING(cc.a1) != '{"key": "__debezium_unavailable_value"}' THEN cc.a1 ELSE c.a1 END,b2= CASE WHEN cc.b2 != '__debezium_unavailable_value' THEN cc.b2 ELSE c.b2 END,c3=cc.c3`,
		},
	}

	for _, _testCase := range testCases {
		actualQuery := ColumnsUpdateQuery(_testCase.columns, _testCase.columnsToTypes, _testCase.bigQuery)
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}

}

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
