package motherduck

import (
	"context"
	"database/sql/driver"
	"fmt"

	duckdb "github.com/duckdb/duckdb-go/v2"
	jsoniter "github.com/json-iterator/go"

	"github.com/artie-labs/transfer/clients/motherduck/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func (s Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tableID, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tableID); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	return appendRows(ctx, s, tableData, tableID)
}

func appendRows(ctx context.Context, store Store, tableData *optimization.TableData, tableID sql.TableIdentifier) error {
	if len(tableData.Rows()) == 0 {
		return nil
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	if len(cols) == 0 {
		return fmt.Errorf("no valid columns to insert")
	}

	castedTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast table identifier to dialect.TableIdentifier")
	}

	conn, err := store.Store.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	var appender *duckdb.Appender
	err = conn.Raw(func(driverConn any) error {
		var appErr error
		appender, appErr = duckdb.NewAppender(driverConn.(driver.Conn), castedTableID.Database(), castedTableID.Schema(), castedTableID.Table())
		if appErr != nil {
			return fmt.Errorf("failed to create appender: %w", appErr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to create appender: %w", err)
	}
	defer appender.Close()

	rowCount := 0
	for _, row := range tableData.Rows() {
		var rowValues []driver.Value
		for _, col := range cols {
			value, _ := row.GetValue(col.Name())
			convertedValue, err := convertValue(value, col.KindDetails)
			if err != nil {
				return fmt.Errorf("failed to convert value: %w", err)
			}
			rowValues = append(rowValues, convertedValue)
		}

		if err := appender.AppendRow(rowValues...); err != nil {
			return fmt.Errorf("failed to append row: %w", err)
		}
		rowCount++
	}

	if err := appender.Close(); err != nil {
		return fmt.Errorf("failed to close appender: %w", err)
	}

	if expectedRows := tableData.NumberOfRows(); uint(rowCount) != expectedRows {
		return fmt.Errorf("expected %d rows to be loaded, but got %d", expectedRows, rowCount)
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
		// For arrays, DuckDB appender expects a Go slice, not a JSON string
		// If it's already a slice, return as-is
		// If it's a string (JSON), parse it into a slice
		switch v := value.(type) {
		case []interface{}:
			return v, nil
		case []string:
			// Convert to []interface{} for DuckDB appender
			result := make([]interface{}, len(v))
			for i, s := range v {
				result[i] = s
			}
			return result, nil
		case string:
			// Parse JSON string into a slice for DuckDB appender
			var arr []interface{}
			if err := json.Unmarshal([]byte(v), &arr); err != nil {
				// If it's not valid JSON, return as a single-element array
				return []interface{}{v}, nil
			}
			return arr, nil
		default:
			// For other types, try to convert to string then parse
			str, err := values.ToString(value, kd)
			if err != nil {
				return nil, err
			}
			// Try to parse as JSON array
			var arr []interface{}
			if err := json.Unmarshal([]byte(str), &arr); err != nil {
				// If it's not valid JSON, return as a single-element array
				return []interface{}{str}, nil
			}
			return arr, nil
		}
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
