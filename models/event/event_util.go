package event

import (
	"encoding/json"
	"math"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/cryptography"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// normalizeNumericVal ensures that json.Number, float64, and int64 representing the same
// logical integer produce the same output from fmt.Sprintf("%v", ...). Without this, Go's
// %v formats json.Number via its String() method (e.g. "1771359601407") but float64 via
// strconv.FormatFloat with 'g' format (e.g. "1.771359601407e+12"), causing identical values
// to generate different in-memory dedup keys when one event has a Debezium schema and another doesn't.
func normalizeNumericVal(value any) any {
	switch v := value.(type) {
	case json.Number:
		if intVal, err := v.Int64(); err == nil {
			return intVal
		}
		if floatVal, err := v.Float64(); err == nil {
			return floatVal
		}
	case float64:
		if v == math.Trunc(v) && !math.IsInf(v, 0) && v >= -(1<<53) && v <= (1<<53) {
			return int64(v)
		}
	}
	return value
}

func buildColumns(event cdc.Event, tc kafkalib.TopicConfig, reservedColumns map[string]bool) ([]columns.Column, error) {
	eventCols, err := event.GetColumns(reservedColumns)
	if err != nil {
		return nil, err
	}

	cols := columns.NewColumns(eventCols)
	for _, col := range tc.ColumnsToExclude {
		cols.DeleteColumn(col)
	}

	if len(tc.ColumnsToInclude) > 0 {
		filteredColumns := columns.NewColumns(nil)
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

		return filteredColumns.GetColumns(), nil
	}

	// Include static columns
	for _, col := range tc.StaticColumns {
		cols.AddColumn(columns.NewColumn(col.Name, typing.String))
	}

	return cols.GetColumns(), nil
}

func buildPrimaryKeys(tc kafkalib.TopicConfig, pkMap map[string]any, reservedColumns map[string]bool) []string {
	var pks []string
	if len(tc.PrimaryKeysOverride) > 0 {
		for _, pk := range tc.PrimaryKeysOverride {
			pks = append(pks, columns.EscapeName(pk, reservedColumns))
		}

		return pks
	}

	// [pkMap] is already escaped.
	for pk := range pkMap {
		pks = append(pks, pk)
	}

	for _, pk := range tc.IncludePrimaryKeys {
		escapedPk := columns.EscapeName(pk, reservedColumns)
		if _, ok := pkMap[escapedPk]; !ok {
			pks = append(pks, escapedPk)
		}
	}

	return pks
}

func transformData(data map[string]any, tc kafkalib.TopicConfig) map[string]any {
	for _, columnToHash := range tc.ColumnsToHash {
		if value, ok := data[columnToHash]; ok {
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

		for _, col := range tc.StaticColumns {
			filteredData[col.Name] = col.Value
		}

		return filteredData
	}

	return data
}

func buildEventData(event cdc.Event, tc kafkalib.TopicConfig) (map[string]any, error) {
	data, err := event.GetData(tc)
	if err != nil {
		return nil, err
	}

	if tc.IncludeArtieOperation {
		data[constants.OperationColumnMarker] = string(event.Operation())
	}

	if tc.IncludeFullSourceTableName {
		data[constants.FullSourceTableNameColumnMarker] = event.GetFullTableName()
	}

	return data, nil
}
