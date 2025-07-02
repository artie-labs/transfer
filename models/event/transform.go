package event

import (
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func transformData(data map[string]any, tc kafkalib.TopicConfig) map[string]any {
	for _, columnToHash := range tc.ColumnsToHash {
		if value, isOk := data[columnToHash]; isOk {
			data[columnToHash] = cryptography.HashValue(value)
		}
	}

	// Exclude certain columns
	for _, col := range tc.ColumnsToExclude {
		delete(data, col)
	}

	// If column inclusion is specified, then we need to include only the specified columns
	if len(tc.ColumnsToInclude) > 0 {
		filteredData := make(map[string]any)
		for _, col := range tc.ColumnsToInclude {
			if value, ok := data[col]; ok {
				filteredData[col] = value
			}
		}

		// Include Artie columns
		for _, col := range constants.ArtieColumns {
			if value, ok := data[col]; ok {
				filteredData[col] = value
			}
		}

		return filteredData
	}

	return data
}

func buildFilteredColumns(event cdc.Event, tc kafkalib.TopicConfig) (*columns.Columns, error) {
	cols, err := event.GetColumns()
	if err != nil {
		return nil, err
	}

	for _, col := range tc.ColumnsToExclude {
		cols.DeleteColumn(col)
	}

	if len(tc.ColumnsToInclude) > 0 {
		var filteredColumns columns.Columns
		for _, col := range tc.ColumnsToInclude {
			if existingColumn, ok := cols.GetColumn(col); ok {
				filteredColumns.AddColumn(existingColumn)
			}
		}

		for _, col := range constants.ArtieColumns {
			if existingColumn, ok := cols.GetColumn(col); ok {
				filteredColumns.AddColumn(existingColumn)
			}
		}

		return &filteredColumns, nil
	}

	return cols, nil
}
