package columns

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

func TestShouldSkipColumn(t *testing.T) {
	{
		// Test delete column marker with soft delete enabled
		assert.False(t, shouldSkipColumn(constants.DeleteColumnMarker, []string{constants.DeleteColumnMarker}))
	}
	{
		// Test delete column marker without soft delete
		assert.True(t, shouldSkipColumn(constants.DeleteColumnMarker, []string{}))
	}
	{
		// Test only set delete column marker
		assert.True(t, shouldSkipColumn(constants.OnlySetDeleteColumnMarker, []string{}))
	}
	{
		// Test only set delete column marker with soft delete enabled
		assert.True(t, shouldSkipColumn(constants.OnlySetDeleteColumnMarker, []string{constants.DeleteColumnMarker}))
	}
	{
		// Test update column marker with artie updated at enabled
		assert.False(t, shouldSkipColumn(constants.UpdateColumnMarker, []string{constants.UpdateColumnMarker}))
	}
	{
		// Test update column marker without artie updated at
		assert.True(t, shouldSkipColumn(constants.UpdateColumnMarker, []string{}))
	}
	{
		// Test regular column
		assert.False(t, shouldSkipColumn("firstName", []string{}))
	}
	{
		// Test regular column with artie updated at and soft delete enabled
		assert.False(t, shouldSkipColumn("email", []string{constants.UpdateColumnMarker, constants.DeleteColumnMarker}))
	}
	{
		// Test database updated column without database updated at enabled
		assert.True(t, shouldSkipColumn(constants.DatabaseUpdatedColumnMarker, []string{}))
	}
	{
		// Test database updated column with database updated at enabled
		assert.False(t, shouldSkipColumn(constants.DatabaseUpdatedColumnMarker, []string{constants.DatabaseUpdatedColumnMarker}))
	}
	{
		// Test source metadata
		assert.False(t, shouldSkipColumn(constants.SourceMetadataColumnMarker, []string{constants.DeleteColumnMarker, constants.SourceMetadataColumnMarker}))
		assert.True(t, shouldSkipColumn(constants.SourceMetadataColumnMarker, []string{constants.DeleteColumnMarker}))
	}
	{
		// Test full source table name
		assert.False(t, shouldSkipColumn(constants.FullSourceTableNameColumnMarker, []string{constants.UpdateColumnMarker, constants.FullSourceTableNameColumnMarker}))
		assert.True(t, shouldSkipColumn(constants.FullSourceTableNameColumnMarker, []string{constants.UpdateColumnMarker}))
	}
	{
		// Test operation column in replication mode
		assert.True(t, shouldSkipColumn(constants.OperationColumnMarker, []string{}))
	}
	{
		// Test operation column in history mode
		assert.False(t, shouldSkipColumn(constants.OperationColumnMarker, []string{constants.OperationColumnMarker}))
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
		sourceKeysMissing, targKeysMissing := DiffAndFilter(columns, columns, []string{})
		assert.Len(t, sourceKeysMissing, 0)
		assert.Len(t, targKeysMissing, 0)
	}
	{
		// Source column has an extra column
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		targCols := []Column{NewColumn("a", typing.String)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, []string{})
		assert.Len(t, sourceKeysMissing, 0)
		assert.Len(t, targKeysMissing, 1)
		assert.Equal(t, targKeysMissing[0], NewColumn("b", typing.Boolean))
	}
	{
		// Destination has an extra column
		sourceCols := []Column{NewColumn("a", typing.String)}
		targCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, []string{})
		assert.Len(t, sourceKeysMissing, 1)
		assert.Equal(t, sourceKeysMissing[0], NewColumn("b", typing.Boolean))
		assert.Len(t, targKeysMissing, 0)
	}
	{
		// Source and destination both have different columns
		sourceCols := []Column{NewColumn("a", typing.String), NewColumn("b", typing.Boolean)}
		targCols := []Column{NewColumn("c", typing.String), NewColumn("d", typing.Boolean)}

		sourceKeysMissing, targKeysMissing := DiffAndFilter(sourceCols, targCols, []string{})
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
