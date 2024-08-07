package columns

import (
	"strings"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/config/constants"
)

// shouldSkipColumn takes the `colName` and `softDelete` and will return whether we should skip this column when calculating the diff.
func shouldSkipColumn(colName string, softDelete bool, includeArtieUpdatedAt bool, includeDatabaseUpdatedAt bool, mode config.Mode) bool {
	// TODO: Figure out a better way to not pass in so many variables when calculating shouldSkipColumn
	if colName == constants.DeleteColumnMarker && softDelete {
		// We need this column to be created if soft deletion is turned on.
		return false
	}

	if colName == constants.OnlySetDeleteColumnMarker {
		// We never want to create this column in the destination table
		return true
	}

	if colName == constants.UpdateColumnMarker && includeArtieUpdatedAt {
		// We want to keep this column if includeArtieUpdatedAt is turned on
		return false
	}

	if colName == constants.DatabaseUpdatedColumnMarker && includeDatabaseUpdatedAt {
		// We want to keep this column if includeDatabaseUpdatedAt is turned on
		return false
	}

	if colName == constants.OperationColumnMarker && mode == config.History {
		return false
	}

	return strings.Contains(colName, constants.ArtiePrefix)
}

// Diff - when given 2 maps, a source and target
// It will provide a diff in the form of 2 variables
func Diff(columnsInSource *Columns, columnsInDestination *Columns, softDelete bool, includeArtieUpdatedAt bool, includeDatabaseUpdatedAt bool, mode config.Mode) ([]Column, []Column) {
	src := CloneColumns(columnsInSource)
	targ := CloneColumns(columnsInDestination)
	var colsToDelete []Column
	for _, col := range src.GetColumns() {
		_, isOk := targ.GetColumn(col.Name())
		if isOk {
			colsToDelete = append(colsToDelete, col)

		}
	}

	// We cannot delete inside a for-loop that is iterating over src.GetColumns() because we are messing up the array order.
	for _, colToDelete := range colsToDelete {
		src.DeleteColumn(colToDelete.Name())
		targ.DeleteColumn(colToDelete.Name())
	}

	var targetColumnsMissing Columns
	for _, col := range src.GetColumns() {
		if shouldSkipColumn(col.Name(), softDelete, includeArtieUpdatedAt, includeDatabaseUpdatedAt, mode) {
			continue
		}

		targetColumnsMissing.AddColumn(col)
	}

	var sourceColumnsMissing Columns
	for _, col := range targ.GetColumns() {
		if shouldSkipColumn(col.Name(), softDelete, includeArtieUpdatedAt, includeDatabaseUpdatedAt, mode) {
			continue
		}

		sourceColumnsMissing.AddColumn(col)
	}

	return sourceColumnsMissing.GetColumns(), targetColumnsMissing.GetColumns()
}

func CloneColumns(cols *Columns) *Columns {
	var newCols Columns
	for _, col := range cols.GetColumns() {
		col.ToLowerName()
		newCols.AddColumn(col)
	}

	return &newCols
}
