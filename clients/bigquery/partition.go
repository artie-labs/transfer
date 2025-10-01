package bigquery

import (
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

func buildDistinctDates(colName string, rows []optimization.Row) ([]string, error) {
	dateMap := make(map[string]bool)
	for _, row := range rows {
		val, ok := row.GetValue(colName)
		if !ok {
			return nil, fmt.Errorf("column %q does not exist in row: %v", colName, row)
		}

		_time, err := typing.ParseDateFromAny(val)
		if err != nil {
			return nil, fmt.Errorf("column %q is not a time column, value: %v, err: %w", colName, val, err)
		}

		dateMap[_time.Format(time.DateOnly)] = true
	}

	return slices.Collect(maps.Keys(dateMap)), nil
}
