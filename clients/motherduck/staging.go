package motherduck

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/artie-labs/ducktape/api/pkg/ducktape"

	"github.com/artie-labs/transfer/clients/motherduck/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/values"
)

func (s Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tableID, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tableID); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	return appendRows(ctx, s, tableData, dwh, tableID)
}

func appendRows(ctx context.Context, store Store, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tableID sql.TableIdentifier) error {
	if len(tableData.Rows()) == 0 {
		return nil
	}

	// For temporary tables, we need to use the in-memory column order because that's how they were created.
	// For permanent tables, dwh already contains columns in the destination table's order from GetTableConfig.
	var cols []columns.Column
	if tableID.TemporaryTable() {
		// Temporary tables are created using tableData columns, so use that order
		cols = tableData.ReadOnlyInMemoryCols().ValidColumns()
	} else {
		// For permanent tables, dwh already contains columns in destination table order.
		// This is populated by GetTableConfig which calls describeTable to get the actual column order.
		cols = dwh.GetColumns()
	}

	if len(cols) == 0 {
		return fmt.Errorf("no valid columns to insert")
	}

	castedTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast table identifier to dialect.TableIdentifier")
	}

	streamIterator := func(yield func(ducktape.RowMessageResult) bool) {
		for _, row := range tableData.Rows() {
			var rowValues []any
			for _, col := range cols {
				// Skip columns that should not be included (e.g., invalid columns)
				if col.ShouldSkip() {
					continue
				}

				value, _ := row.GetValue(col.Name())
				convertedValue, err := convertValue(value, col.KindDetails)
				if err != nil {
					errMsg := fmt.Sprintf("failed to convert value while appending: %v", err)
					yield(ducktape.RowMessageResult{Error: &errMsg})
					return
				}
				rowValues = append(rowValues, convertedValue)
			}
			if !yield(ducktape.RowMessageResult{Row: ducktape.RowMessage{Values: rowValues}}) {
				return
			}
		}
	}

	resp, err := store.client.Append(
		ctx,
		store.dsn,
		castedTableID.Database(),
		castedTableID.Schema(),
		castedTableID.Table(),
		streamIterator,
		func(r ducktape.RowMessage) ([]byte, error) {
			return json.Marshal(r)
		},
		func(r []byte) (*ducktape.AppendResponse, error) {
			var resp ducktape.AppendResponse
			if err := json.Unmarshal(r, &resp); err != nil {
				return nil, err
			}
			return &resp, nil
		},
	)
	if err != nil {
		return fmt.Errorf("failure on client side to append rows: %w", err)
	}
	if resp.Error != nil {
		return fmt.Errorf("failure on server side to append rows: %s", *resp.Error)
	}

	if expectedRows := tableData.NumberOfRows(); resp.RowsAppended != int64(expectedRows) {
		return fmt.Errorf("expected %d rows to be loaded, but got %d", expectedRows, resp.RowsAppended)
	}

	return nil
}

func convertValue(value any, kd typing.KindDetails) (driver.Value, error) {
	if value == nil {
		return nil, nil
	}

	switch kd.Kind {
	case typing.String.Kind:
		castedValue, err := typing.AssertType[string](value)
		if err != nil {
			return "", err
		}
		return castedValue, nil
	case typing.Boolean.Kind:
		castedValue, err := typing.AssertType[bool](value)
		if err != nil {
			return nil, err
		}
		return castedValue, nil
	case typing.Struct.Kind:
		// For structs, convert to JSON string
		str, err := values.ToString(value, kd)
		if err != nil {
			return nil, err
		}
		return str, nil
	case typing.Array.Kind:
		arrayStr, err := array.InterfaceToArrayString(value, true)
		if err != nil {
			return nil, fmt.Errorf("failed to convert array: %w", err)
		}
		return arrayStr, nil
	case typing.Integer.Kind, typing.Float.Kind:
		// Return as-is, DuckDB appender will handle conversion
		return value, nil
	case typing.EDecimal.Kind:
		// Convert decimal to string for DuckDB
		str, err := values.ToString(value, kd)
		if err != nil {
			return nil, err
		}
		return str, nil
	case typing.Date.Kind:
		// Parse date strings into time.Time for DuckDB appender
		timeVal, err := typing.ParseDateFromAny(value)
		if err != nil {
			return nil, err
		}
		return timeVal, nil
	case typing.Time.Kind:
		// Parse time strings into time.Time for DuckDB appender
		timeVal, err := typing.ParseTimeFromAny(value)
		if err != nil {
			return nil, err
		}
		return timeVal, nil
	case typing.TimestampNTZ.Kind:
		// Parse timestamp without timezone into time.Time for DuckDB appender
		timeVal, err := typing.ParseTimestampNTZFromAny(value)
		if err != nil {
			return nil, err
		}
		return timeVal, nil
	case typing.TimestampTZ.Kind:
		// Parse timestamp with timezone into time.Time for DuckDB appender
		timeVal, err := typing.ParseTimestampTZFromAny(value)
		if err != nil {
			return nil, err
		}
		return timeVal, nil
	default:
		// For any other types, return as-is and let DuckDB handle it
		return value, nil
	}
}
