package bigquery

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// [buildDistinctDates] - Builds a list of distinct dates from the rows for BigQuery date partitioning.
func buildDistinctDates(colName string, rows []optimization.Row, reservedColumnNames map[string]bool) ([]string, error) {
	dateMap := make(map[string]bool)
	colName = columns.EscapeName(colName, reservedColumnNames)
	for _, row := range rows {
		val, ok := row.GetValue(colName)
		if !ok || val == nil {
			// Skip distinct date filtering if there are nil or missing values. They will all end up in the `__NULL__` partition.
			return nil, nil
		}

		_time, err := typing.ParseDateFromAny(val)
		if err != nil {
			return nil, fmt.Errorf("column %q is not a time column, value: %v, err: %w", colName, val, err)
		}

		dateMap[_time.Format(time.DateOnly)] = true
	}

	return slices.Collect(maps.Keys(dateMap)), nil
}
