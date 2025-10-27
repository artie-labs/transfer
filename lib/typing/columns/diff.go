package columns

import (
	"slices"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/maputil"
)

func shouldSkipColumn(colName string, columnsToKeep []string) bool {
	if slices.Contains(columnsToKeep, colName) {
		return false
	}

	if colName == constants.OnlySetDeleteColumnMarker {
		// We never want to create this column in the destination table
		return true
	}

	return strings.Contains(colName, constants.ArtiePrefix)
}

type DiffResults struct {
	SourceColumnsMissing []Column
	TargetColumnsMissing []Column
}

func Diff(sourceColumns, targetColumns []Column) DiffResults {
	src := buildColumnsMap(sourceColumns)
	targ := buildColumnsMap(targetColumns)

	for _, colName := range src.Keys() {
		if _, ok := targ.Get(colName); ok {
			targ.Remove(colName)
			src.Remove(colName)
		}
	}

	var targetColumnsMissing []Column
	for _, col := range src.All() {
		targetColumnsMissing = append(targetColumnsMissing, col)
	}

	var sourceColumnsMissing []Column
	for _, col := range targ.All() {
		sourceColumnsMissing = append(sourceColumnsMissing, col)
	}

	return DiffResults{
		SourceColumnsMissing: sourceColumnsMissing,
		TargetColumnsMissing: targetColumnsMissing,
	}
}

func filterColumns(columns []Column, columnsToKeep []string) []Column {
	var filteredColumns []Column
	for _, col := range columns {
		if shouldSkipColumn(col.Name(), columnsToKeep) {
			continue
		}

		filteredColumns = append(filteredColumns, col)
	}

	return filteredColumns
}

// DiffAndFilter - will diff the columns and filter out any Artie metadata columns that should not exist in the target table.
func DiffAndFilter(columnsInSource, columnsInDestination []Column, columnsToKeep []string) ([]Column, []Column) {
	diffResult := Diff(columnsInSource, columnsInDestination)
	return filterColumns(diffResult.SourceColumnsMissing, columnsToKeep), filterColumns(diffResult.TargetColumnsMissing, columnsToKeep)
}

func buildColumnsMap(cols []Column) *maputil.OrderedMap[Column] {
	retMap := maputil.NewOrderedMap[Column](false)
	for _, col := range cols {
		retMap.Add(col.name, col)
	}

	return retMap
}
