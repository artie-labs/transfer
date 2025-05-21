package columns

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/stretchr/testify/assert"
)

func TestShouldSkipColumn(t *testing.T) {
	{
		// Test delete column marker with soft delete enabled
		assert.False(t, shouldSkipColumn(constants.DeleteColumnMarker, true, false, false, false, config.Mode("")))
	}
	{
		// Test delete column marker without soft delete
		assert.True(t, shouldSkipColumn(constants.DeleteColumnMarker, false, false, false, false, config.Mode("")))
	}
	{
		// Test only set delete column marker
		assert.True(t, shouldSkipColumn(constants.OnlySetDeleteColumnMarker, false, false, false, false, config.Mode("")))
	}
	{
		// Test only set delete column marker with soft delete enabled
		assert.True(t, shouldSkipColumn(constants.OnlySetDeleteColumnMarker, true, false, false, false, config.Mode("")))
	}
	{
		// Test update column marker with artie updated at enabled
		assert.False(t, shouldSkipColumn(constants.UpdateColumnMarker, false, true, false, false, config.Mode("")))
	}
	{
		// Test update column marker without artie updated at
		assert.True(t, shouldSkipColumn(constants.UpdateColumnMarker, false, false, false, false, config.Mode("")))
	}
	{
		// Test regular column
		assert.False(t, shouldSkipColumn("firstName", false, false, false, false, config.Mode("")))
	}
	{
		// Test regular column with artie updated at and soft delete enabled
		assert.False(t, shouldSkipColumn("email", true, true, false, false, config.Mode("")))
	}
	{
		// Test database updated column without database updated at enabled
		assert.True(t, shouldSkipColumn(constants.DatabaseUpdatedColumnMarker, false, false, false, false, config.Mode("")))
	}
	{
		// Test database updated column with database updated at enabled
		assert.False(t, shouldSkipColumn(constants.DatabaseUpdatedColumnMarker, false, false, true, false, config.Mode("")))
	}
	{
		// Test operation column in replication mode
		assert.True(t, shouldSkipColumn(constants.OperationColumnMarker, false, false, false, false, config.Replication))
	}
	{
		// Test operation column in history mode
		assert.False(t, shouldSkipColumn(constants.OperationColumnMarker, false, false, false, false, config.History))
	}
	{
		// Test operation column with includeArtieOperation enabled
		assert.False(t, shouldSkipColumn(constants.OperationColumnMarker, false, false, false, true, config.Replication))
	}
	{
		// Test operation column with includeArtieOperation enabled in history mode
		assert.False(t, shouldSkipColumn(constants.OperationColumnMarker, false, false, false, true, config.History))
	}
	{
		// Test artie prefixed column
		assert.True(t, shouldSkipColumn("__artie_metadata", false, false, false, false, config.Mode("")))
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
		sourceKeysMissing, targKeysMissing := DiffAndFilter(columns, columns, false, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 0)
		assert.Len(t, targKeysMissing, 0)
	}
	{
		// Source column has an extra column
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		targCols := []Column{NewColumn("a", typing.String)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, false, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 0)
		assert.Len(t, targKeysMissing, 1)
		assert.Equal(t, targKeysMissing[0], NewColumn("b", typing.Boolean))
	}
	{
		// Destination has an extra column
		sourceCols := []Column{NewColumn("a", typing.String)}
		targCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, false, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 1)
		assert.Equal(t, sourceKeysMissing[0], NewColumn("b", typing.Boolean))
		assert.Len(t, targKeysMissing, 0)
	}
	{
		// Source and destination both have different columns
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		targCols := []Column{NewColumn("c", typing.String), NewColumn("d", typing.Boolean)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, false, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 2)
		assert.Equal(t, sourceKeysMissing, targCols)
		assert.Len(t, targKeysMissing, 2)
		assert.Equal(t, targKeysMissing, sourceCols)
	}
	{
		// Test with operation column and includeArtieOperation enabled
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn(constants.OperationColumnMarker, typing.String)}
		targCols := []Column{NewColumn("a", typing.String)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, false, false, false, true, config.Replication)
		assert.Len(t, sourceKeysMissing, 0)
		assert.Len(t, targKeysMissing, 1)
		assert.Equal(t, targKeysMissing[0], NewColumn(constants.OperationColumnMarker, typing.String))
	}
	{
		// Test with operation column and includeArtieOperation disabled
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn(constants.OperationColumnMarker, typing.String)}
		targCols := []Column{NewColumn("a", typing.String)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, false, false, false, false, config.Replication)
		assert.Len(t, sourceKeysMissing, 0)
		assert.Len(t, targKeysMissing, 0) // Operation column should be filtered out
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
