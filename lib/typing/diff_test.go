package typing

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
	"testing"
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

func TestDiffTargNil(t *testing.T) {
	var sourceCols Columns
	var targCols Columns

	sourceCols.AddColumn(Column{
		Name:        "foo",
		KindDetails: Invalid,
	})
	srcKeyMissing, targKeyMissing := Diff(sourceCols, targCols, false)
	assert.Equal(t, len(srcKeyMissing), 0)
	assert.Equal(t, len(targKeyMissing), 1)
}

func TestDiffSourceNil(t *testing.T) {
	var sourceCols Columns
	var targCols Columns

	targCols.AddColumn(Column{
		Name:        "foo",
		KindDetails: Invalid,
	})

	srcKeyMissing, targKeyMissing := Diff(sourceCols, targCols, false)
	assert.Equal(t, len(srcKeyMissing), 1)
	assert.Equal(t, len(targKeyMissing), 0)
}

func TestDiffBasic(t *testing.T) {
	var source Columns
	source.AddColumn(Column{
		Name:        "a",
		KindDetails: Integer,
	})

	srcKeyMissing, targKeyMissing := Diff(source, source, false)
	assert.Equal(t, len(srcKeyMissing), 0)
	assert.Equal(t, len(targKeyMissing), 0)
}

func TestDiffDelta1(t *testing.T) {
	var sourceCols Columns
	var targCols Columns
	for colName, kindDetails := range map[string]KindDetails{
		"a": String,
		"b": Boolean,
		"c": Struct,
	} {
		sourceCols.AddColumn(Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	for colName, kindDetails := range map[string]KindDetails{
		"aa": String,
		"b":  Boolean,
		"cc": String,
	} {
		targCols.AddColumn(Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	srcKeyMissing, targKeyMissing := Diff(sourceCols, targCols, false)
	assert.Equal(t, len(srcKeyMissing), 2, srcKeyMissing)   // Missing aa, cc
	assert.Equal(t, len(targKeyMissing), 2, targKeyMissing) // Missing aa, cc
}

func TestDiffDelta2(t *testing.T) {
	var sourceCols Columns
	var targetCols Columns

	for colName, kindDetails := range map[string]KindDetails{
		"a":  String,
		"aa": String,
		"b":  Boolean,
		"c":  Struct,
		"d":  String,
		"CC": String,
		"cC": String,
		"Cc": String,
	} {
		sourceCols.AddColumn(Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	for colName, kindDetails := range map[string]KindDetails{
		"aa": String,
		"b":  Boolean,
		"cc": String,
		"CC": String,
		"dd": String,
	} {
		targetCols.AddColumn(Column{
			Name:        colName,
			KindDetails: kindDetails,
		})
	}

	srcKeyMissing, targKeyMissing := Diff(sourceCols, targetCols, false)
	assert.Equal(t, len(srcKeyMissing), 1, srcKeyMissing)   // Missing dd
	assert.Equal(t, len(targKeyMissing), 3, targKeyMissing) // Missing a, c, d
}

func TestDiffDeterministic(t *testing.T) {
	retMap := map[string]bool{}

	var sourceCols Columns
	var targCols Columns

	sourceCols.AddColumn(Column{
		Name:        "id",
		KindDetails: Integer,
	})

	sourceCols.AddColumn(Column{
		Name:        "name",
		KindDetails: String,
	})

	for i := 0; i < 500; i++ {
		keysMissing, targetKeysMissing := Diff(sourceCols, targCols, false)
		assert.Equal(t, 0, len(keysMissing), keysMissing)

		var key string
		for _, targetKeyMissing := range targetKeysMissing {
			key += targetKeyMissing.Name
		}

		retMap[key] = false
	}

	assert.Equal(t, 1, len(retMap), retMap)
}

func TestCopyColMap(t *testing.T) {
	var cols Columns
	cols.AddColumn(Column{
		Name:        "hello",
		KindDetails: String,
	})
	cols.AddColumn(Column{
		Name:        "created_at",
		KindDetails: NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
	})
	cols.AddColumn(Column{
		Name:        "updated_at",
		KindDetails: NewKindDetailsFromTemplate(ETime, ext.DateTimeKindType),
	})

	copiedCols := CloneColumns(cols)
	assert.Equal(t, copiedCols, cols)

	//Delete a row from copiedCols
	copiedCols.columns = append(copiedCols.columns[1:])
	assert.NotEqual(t, copiedCols, cols)
}
