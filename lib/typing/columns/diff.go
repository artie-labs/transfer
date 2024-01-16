package columns

import (
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

// shouldSkipColumn takes the `colName` and `softDelete` and will return whether we should skip this column when calculating the diff.
func shouldSkipColumn(colName string, softDelete bool, includeArtieUpdatedAt bool) bool {
	if colName == constants.DeleteColumnMarker && softDelete {
		// We need this column to be created if soft deletion is turned on.
		return false
	}

	if colName == constants.UpdateColumnMarker && includeArtieUpdatedAt {
		// We want to keep this column if includeArtieUpdatedAt is turned on
		return false
	}

	if strings.Contains(colName, constants.ArtiePrefix) {
		return true
	}

	return false
}

// Diff - when given 2 maps, a source and target
// It will provide a diff in the form of 2 variables
func Diff(columnsInSource *Columns, columnsInDestination *Columns, softDelete bool, includeArtieUpdatedAt bool) ([]Column, []Column) {
	src := CloneColumns(columnsInSource)
	targ := CloneColumns(columnsInDestination)
	var colsToDelete []Column
	for _, col := range src.GetColumns() {
		_, isOk := targ.GetColumn(col.RawName())
		if isOk {
			colsToDelete = append(colsToDelete, col)

		}
	}

	// We cannot delete inside a for-loop that is iterating over src.GetColumns() because we are messing up the array order.
	for _, colToDelete := range colsToDelete {
		src.DeleteColumn(colToDelete.RawName())
		targ.DeleteColumn(colToDelete.RawName())
	}

	var targetColumnsMissing Columns
	for _, col := range src.GetColumns() {
		if shouldSkipColumn(col.RawName(), softDelete, includeArtieUpdatedAt) {
			continue
		}

		targetColumnsMissing.AddColumn(col)
	}

	var sourceColumnsMissing Columns
	for _, col := range targ.GetColumns() {
		if shouldSkipColumn(col.RawName(), softDelete, includeArtieUpdatedAt) {
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
