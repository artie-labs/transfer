package columns

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestShouldSkipColumn(t *testing.T) {
	colsToExpectation := map[string]bool{
		"id":                         false,
		"21331":                      false,
		constants.DeleteColumnMarker: true,
		fmt.Sprintf("%s_hellooooooo", constants.ArtiePrefix): true,
	}

	for col, expected := range colsToExpectation {
		assert.Equal(t, shouldSkipColumn(col, false), expected)
	}

	// When toggling soft deletion on, this column should not be skipped.
	colsToExpectation[constants.DeleteColumnMarker] = false
	for col, expected := range colsToExpectation {
		assert.Equal(t, shouldSkipColumn(col, true), expected)
	}
}

func TestDiff_VariousNils(t *testing.T) {
	type _testCase struct {
		name       string
		sourceCols *Columns
		targCols   *Columns

		expectedSrcKeyLength  int
		expectedTargKeyLength int
	}

	var sourceColsNotNil Columns
	var targColsNotNil Columns
	sourceColsNotNil.AddColumn(NewColumn("foo", typing.Invalid))
	targColsNotNil.AddColumn(NewColumn("foo", typing.Invalid))
	testCases := []_testCase{
		{
			name:       "both &Columns{}",
			sourceCols: &Columns{},
			targCols:   &Columns{},
		},
		{
			name:                  "only targ is &Columns{}",
			sourceCols:            &sourceColsNotNil,
			targCols:              &Columns{},
			expectedTargKeyLength: 1,
		},
		{
			name:                 "only source is &Columns{}",
			sourceCols:           &Columns{},
			targCols:             &targColsNotNil,
			expectedSrcKeyLength: 1,
		},
		{
			name:       "both nil",
			sourceCols: nil,
			targCols:   nil,
		},
		{
			name:                  "only targ is nil",
			sourceCols:            &sourceColsNotNil,
			targCols:              nil,
			expectedTargKeyLength: 1,
		},
		{
			name:                 "only source is nil",
			sourceCols:           nil,
			targCols:             &targColsNotNil,
			expectedSrcKeyLength: 1,
		},
	}

	for _, testCase := range testCases {
		actualSrcKeysMissing, actualTargKeysMissing := Diff(testCase.sourceCols, testCase.targCols, false)
		assert.Equal(t, testCase.expectedSrcKeyLength, len(actualSrcKeysMissing), testCase.name)
		assert.Equal(t, testCase.expectedTargKeyLength, len(actualTargKeysMissing), testCase.name)
	}
}

func TestDiffBasic(t *testing.T) {
	var source Columns
	source.AddColumn(NewColumn("a", typing.Integer))

	srcKeyMissing, targKeyMissing := Diff(&source, &source, false)
	assert.Equal(t, len(srcKeyMissing), 0)
	assert.Equal(t, len(targKeyMissing), 0)
}

func TestDiffDelta1(t *testing.T) {
	var sourceCols Columns
	var targCols Columns
	for colName, kindDetails := range map[string]typing.KindDetails{
		"a": typing.String,
		"b": typing.Boolean,
		"c": typing.Struct,
	} {
		sourceCols.AddColumn(NewColumn(colName, kindDetails))
	}

	for colName, kindDetails := range map[string]typing.KindDetails{
		"aa": typing.String,
		"b":  typing.Boolean,
		"cc": typing.String,
	} {
		targCols.AddColumn(NewColumn(colName, kindDetails))
	}

	srcKeyMissing, targKeyMissing := Diff(&sourceCols, &targCols, false)
	assert.Equal(t, len(srcKeyMissing), 2, srcKeyMissing)   // Missing aa, cc
	assert.Equal(t, len(targKeyMissing), 2, targKeyMissing) // Missing aa, cc
}

func TestDiffDelta2(t *testing.T) {
	var sourceCols Columns
	var targetCols Columns

	for colName, kindDetails := range map[string]typing.KindDetails{
		"a":  typing.String,
		"aa": typing.String,
		"b":  typing.Boolean,
		"c":  typing.Struct,
		"d":  typing.String,
		"CC": typing.String,
		"cC": typing.String,
		"Cc": typing.String,
	} {
		sourceCols.AddColumn(NewColumn(colName, kindDetails))
	}

	for colName, kindDetails := range map[string]typing.KindDetails{
		"aa": typing.String,
		"b":  typing.Boolean,
		"cc": typing.String,
		"CC": typing.String,
		"dd": typing.String,
	} {
		targetCols.AddColumn(NewColumn(colName, kindDetails))
	}

	srcKeyMissing, targKeyMissing := Diff(&sourceCols, &targetCols, false)
	assert.Equal(t, len(srcKeyMissing), 1, srcKeyMissing)   // Missing dd
	assert.Equal(t, len(targKeyMissing), 3, targKeyMissing) // Missing a, c, d
}

func TestDiffDeterministic(t *testing.T) {
	retMap := map[string]bool{}

	var sourceCols Columns
	var targCols Columns

	sourceCols.AddColumn(NewColumn("id", typing.Integer))
	sourceCols.AddColumn(NewColumn("name", typing.String))

	for i := 0; i < 500; i++ {
		keysMissing, targetKeysMissing := Diff(&sourceCols, &targCols, false)
		assert.Equal(t, 0, len(keysMissing), keysMissing)

		var key string
		for _, targetKeyMissing := range targetKeysMissing {
			key += targetKeyMissing.Name(nil)
		}

		retMap[key] = false
	}

	assert.Equal(t, 1, len(retMap), retMap)
}

func TestCopyColMap(t *testing.T) {
	var cols Columns
	cols.AddColumn(NewColumn("hello", typing.String))
	cols.AddColumn(NewColumn("created_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)))
	cols.AddColumn(NewColumn("updated_at", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)))

	copiedCols := CloneColumns(&cols)
	assert.Equal(t, *copiedCols, cols)

	//Delete a row from copiedCols
	copiedCols.columns = append(copiedCols.columns[1:])
	assert.NotEqual(t, *copiedCols, cols)
}

func TestCloneColumns(t *testing.T) {
	type _testCase struct {
		name         string
		cols         *Columns
		expectedCols *Columns
	}

	var cols Columns
	cols.AddColumn(NewColumn("foo", typing.String))
	cols.AddColumn(NewColumn("bar", typing.String))
	cols.AddColumn(NewColumn("xyz", typing.String))
	cols.AddColumn(NewColumn("abc", typing.String))

	var mixedCaseCols Columns
	mixedCaseCols.AddColumn(NewColumn("foo", typing.String))
	mixedCaseCols.AddColumn(NewColumn("bAr", typing.String))
	mixedCaseCols.AddColumn(NewColumn("XYZ", typing.String))
	mixedCaseCols.AddColumn(NewColumn("aBC", typing.String))

	testCases := []_testCase{
		{
			name:         "nil col",
			expectedCols: &Columns{},
		},
		{
			name:         "&Columns{}",
			cols:         &Columns{},
			expectedCols: &Columns{},
		},
		{
			name:         "copying columns",
			cols:         &cols,
			expectedCols: &cols,
		},
		{
			name:         "mixed case cols",
			cols:         &mixedCaseCols,
			expectedCols: &cols,
		},
	}

	for _, testCase := range testCases {
		actualCols := CloneColumns(testCase.cols)
		assert.Equal(t, *testCase.expectedCols, *actualCols, testCase.name)
	}
}
