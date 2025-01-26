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

func TestDiff(t *testing.T) {
	{
		// The same columns
		columns := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		diffResult := Diff(columns, columns)
		assert.Empty(t, diffResult.SourceColumnsMissing)
		assert.Empty(t, diffResult.TargetColumnsMissing)
	}
	{
		// Source has an extra column (so target is missing it)
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		targCols := []Column{NewColumn("a", typing.String)}
		diffResult := Diff(sourceCols, targCols)
		assert.Len(t, diffResult.TargetColumnsMissing, 1)
		assert.Equal(t, diffResult.TargetColumnsMissing[0], NewColumn("b", typing.Boolean))
		assert.Empty(t, diffResult.SourceColumnsMissing)
	}
	{
		// Destination has an extra column (so source is missing it)
		sourceCols := []Column{NewColumn("a", typing.String)}
		targCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		diffResult := Diff(sourceCols, targCols)
		assert.Empty(t, diffResult.TargetColumnsMissing)
		assert.Len(t, diffResult.SourceColumnsMissing, 1)
		assert.Equal(t, diffResult.SourceColumnsMissing[0], NewColumn("b", typing.Boolean))
	}
	{
		// Source and destination have different columns
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		targCols := []Column{NewColumn("c", typing.String), NewColumn("d", typing.Boolean)}
		diffResult := Diff(sourceCols, targCols)
		assert.Len(t, diffResult.SourceColumnsMissing, 2)
		assert.Equal(t, diffResult.SourceColumnsMissing, targCols)
		assert.Len(t, diffResult.TargetColumnsMissing, 2)
		assert.Equal(t, diffResult.TargetColumnsMissing, sourceCols)
	}
}

func TestDiffAndFilter(t *testing.T) {
	{
		// The same columns
		columns := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		sourceKeysMissing, targKeysMissing := DiffAndFilter(columns, columns, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 0)
		assert.Len(t, targKeysMissing, 0)
	}
	{
		// Source column has an extra column
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		targCols := []Column{NewColumn("a", typing.String)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 0)
		assert.Len(t, targKeysMissing, 1)
		assert.Equal(t, targKeysMissing[0], NewColumn("b", typing.Boolean))
	}
	{
		// Destination has an extra column
		sourceCols := []Column{NewColumn("a", typing.String)}
		targCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 1)
		assert.Equal(t, sourceKeysMissing[0], NewColumn("b", typing.Boolean))
		assert.Len(t, targKeysMissing, 0)
	}
	{
		// Source and destination both have different columns
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		targCols := []Column{NewColumn("c", typing.String), NewColumn("d", typing.Boolean)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 2)
		assert.Equal(t, sourceKeysMissing, targCols)
		assert.Len(t, targKeysMissing, 2)
		assert.Equal(t, targKeysMissing, sourceCols)
	}
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
