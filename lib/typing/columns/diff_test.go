package columns

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/stretchr/testify/assert"
)

func TestShouldSkipColumn(t *testing.T) {
	type _testCase struct {
		name                     string
		colName                  string
		softDelete               bool
		includeArtieUpdatedAt    bool
		includeDatabaseUpdatedAt bool
		cfgMode                  config.Mode

		expectedResult bool
	}

	testCases := []_testCase{
		{
			name:       "delete col marker + soft delete",
			colName:    constants.DeleteColumnMarker,
			softDelete: true,
		},
		{
			name:           "delete col marker",
			colName:        constants.DeleteColumnMarker,
			expectedResult: true,
		},
		{
			name:           "only_set_delete col marker should be skipped",
			colName:        constants.OnlySetDeleteColumnMarker,
			expectedResult: true,
		},
		{
			name:           "only_set_delete col marker should be skipped even if softDelete is true",
			colName:        constants.OnlySetDeleteColumnMarker,
			softDelete:     true,
			expectedResult: true,
		},
		{
			name:                  "updated col marker + include updated",
			colName:               constants.UpdateColumnMarker,
			includeArtieUpdatedAt: true,
		},
		{
			name:           "updated col marker",
			colName:        constants.UpdateColumnMarker,
			expectedResult: true,
		},
		{
			name:    "random col",
			colName: "firstName",
		},
		{
			name:                  "col with includeArtieUpdatedAt + softDelete",
			colName:               "email",
			includeArtieUpdatedAt: true,
			softDelete:            true,
		},
		{
			name:                     "db updated at col BUT updated_at is not enabled",
			colName:                  constants.DatabaseUpdatedColumnMarker,
			includeDatabaseUpdatedAt: false,
			expectedResult:           true,
		},
		{
			name:                     "db updated at col AND updated_at is enabled",
			colName:                  constants.DatabaseUpdatedColumnMarker,
			includeDatabaseUpdatedAt: true,
			expectedResult:           false,
		},
		{
			name:           "operation col AND mode is replication mode",
			colName:        constants.OperationColumnMarker,
			cfgMode:        config.Replication,
			expectedResult: true,
		},
		{
			name:           "operation col AND mode is history mode",
			colName:        constants.OperationColumnMarker,
			cfgMode:        config.History,
			expectedResult: false,
		},
	}

	for _, testCase := range testCases {
		actualResult := shouldSkipColumn(testCase.colName, testCase.softDelete, testCase.includeArtieUpdatedAt, testCase.includeDatabaseUpdatedAt, testCase.cfgMode)
		assert.Equal(t, testCase.expectedResult, actualResult, testCase.name)
	}
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

	srcKeyMissing, targKeyMissing := Diff(sourceCols.GetColumns(), targCols.GetColumns(), false, false, false, config.Replication)
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

	srcKeyMissing, targKeyMissing := Diff(sourceCols.GetColumns(), targetCols.GetColumns(), false, false, false, config.Replication)
	assert.Equal(t, len(srcKeyMissing), 1, srcKeyMissing)   // Missing dd
	assert.Equal(t, len(targKeyMissing), 3, targKeyMissing) // Missing a, c, d
}

func TestBuildColumnsMap(t *testing.T) {
	var cols Columns
	cols.AddColumn(NewColumn("hello", typing.String))
	cols.AddColumn(NewColumn("created_at", typing.TimestampTZ))
	cols.AddColumn(NewColumn("updated_at", typing.TimestampTZ))

	copiedCols := buildColumnsMap(cols.GetColumns())
	copiedCols.Remove("hello")

	assert.Len(t, copiedCols.Keys(), 2)
	assert.Len(t, cols.GetColumns(), 3)
}
