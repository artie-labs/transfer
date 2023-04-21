package typing

import (
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
)

// shouldSkipColumn takes the `colName` and `softDelete` and will return whether we should skip this column when calculating the diff.
func shouldSkipColumn(colName string, softDelete bool) bool {
	if colName == constants.DeleteColumnMarker && softDelete {
		// We need this column to be created if soft deletion is turned on.
		return false
	}

	if strings.Contains(colName, constants.ArtiePrefix) {
		return true
	}

	return false
}

// Diff - when given 2 maps, a source and target
// It will provide a diff in the form of 2 variables
// TODO Fix
func Diff(columnsInSource Columns, columnsInDestination Columns, softDelete bool) (sourceColumnsMissing []Column, targetColumnsMissing []Column) {
	src := CloneColumns(columnsInSource)
	targ := CloneColumns(columnsInDestination)
	for _, col := range src.GetColumns() {
		targetCol := targ.GetColumn(col.Name)
		if targetCol != nil {
			src.DeleteColumn(col.Name)
			targ.DeleteColumn(col.Name)
		}
	}

	var sourceColumnsMissingWrapper Columns
	var targetColumnsMissingWrapper Columns

	for _, col := range src.GetColumns() {
		if shouldSkipColumn(col.Name, softDelete) {
			continue
		}

		targetColumnsMissingWrapper.AddColumn(col)
	}

	for _, col := range targ.GetColumns() {
		if shouldSkipColumn(col.Name, softDelete) {
			continue
		}

		sourceColumnsMissingWrapper.AddColumn(col)
	}

	return sourceColumnsMissingWrapper.GetColumns(), targetColumnsMissingWrapper.GetColumns()
}

func CloneColumns(cols Columns) Columns {
	var newCols Columns
	for _, col := range cols.GetColumns() {
		col.Name = strings.ToLower(col.Name)
		newCols.AddColumn(col)
	}

	return newCols
}
