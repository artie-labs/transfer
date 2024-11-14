package columns

import (
	"strings"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/maputil"
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
func Diff(columnsInSource []Column, columnsInDestination []Column, softDelete bool, includeArtieUpdatedAt bool, includeDatabaseUpdatedAt bool, mode config.Mode) ([]Column, []Column) {
	src := buildColumnsMap(columnsInSource)
	targ := buildColumnsMap(columnsInDestination)
	for _, colName := range src.Keys() {
		if _, isOk := targ.Get(colName); isOk {
			targ.Remove(colName)
			src.Remove(colName)
		}
	}

	var targetColumnsMissing []Column
	for _, col := range src.All() {
		if shouldSkipColumn(col.Name(), softDelete, includeArtieUpdatedAt, includeDatabaseUpdatedAt, mode) {
			continue
		}

		targetColumnsMissing = append(targetColumnsMissing, col)
	}

	var sourceColumnsMissing []Column
	for _, col := range targ.All() {
		if shouldSkipColumn(col.Name(), softDelete, includeArtieUpdatedAt, includeDatabaseUpdatedAt, mode) {
			continue
		}

		sourceColumnsMissing = append(sourceColumnsMissing, col)
	}

	return sourceColumnsMissing, targetColumnsMissing
}

func buildColumnsMap(cols []Column) *maputil.OrderedMap[Column] {
	retMap := maputil.NewOrderedMap[Column](false)
	for _, col := range cols {
		retMap.Add(col.name, col)
	}

	return retMap
}
