package event

import (
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func buildColumns(event cdc.Event, tc kafkalib.TopicConfig, reservedColumns map[string]bool) (*columns.Columns, error) {
	cols, err := event.GetColumns(reservedColumns)
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

		// If columns to include is specified, we should always include static columns.
		for _, col := range tc.StaticColumns {
			filteredColumns.AddColumn(columns.NewColumn(col.Name, typing.String))
		}

		return &filteredColumns, nil
	}

	// Include static columns
	for _, col := range tc.StaticColumns {
		cols.AddColumn(columns.NewColumn(col.Name, typing.String))
	}

	return cols, nil
}
