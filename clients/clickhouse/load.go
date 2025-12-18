package clickhouse

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	jsoniter "github.com/json-iterator/go"

	_ "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

var dontWriteArtieColumns = map[string]bool{
	constants.OnlySetDeleteColumnMarker: true,
}

func (s Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tableID, _ sql.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, tableConfig, additionalSettings.ColumnSettings, tableID); err != nil {
			return err
		}
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	if len(cols) == 0 {
		return nil
	}

	// Build column names and placeholders for the INSERT statement
	colNames := []string{}
	placeholders := []string{}
	for _, col := range cols {
		if dontWriteArtieColumns[col.Name()] {
			continue
		}
		colNames = append(colNames, s.Dialect().QuoteIdentifier(col.Name()))
		placeholders = append(placeholders, "?")
	}

	// Build the INSERT query: INSERT INTO table (col1, col2, ...) VALUES (?, ?, ...)
	insertQuery := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableID.FullyQualifiedName(),
		strings.Join(colNames, ", "),
		strings.Join(placeholders, ", "),
	)

	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	var committed bool
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.PrepareContext(ctx, insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert each row using the prepared statement
	for _, row := range tableData.Rows() {
		values := []any{}
		for _, col := range cols {
			if dontWriteArtieColumns[col.Name()] {
				continue
			}
			value, _ := row.GetValue(col.Name())
			parsedValue, err := parseValue(value, col)
			if err != nil {
				return fmt.Errorf("failed to parse value for column %q: %w", col.Name(), err)
			}
			values = append(values, parsedValue)
		}

		if _, err := stmt.ExecContext(ctx, values...); err != nil {
			return fmt.Errorf("failed to execute insert: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	committed = true
	return nil
}

// parseValue converts a value to the appropriate type for ClickHouse insertion
func parseValue(value any, col columns.Column) (any, error) {
	if value == nil {
		// ClickHouse driver requires typed nil values for certain column types
		switch col.KindDetails.Kind {
		case typing.Array.Kind:
			return []string{}, nil
		case typing.Struct.Kind:
			return "{}", nil // JSON columns need empty object, not nil
		}
		return nil, nil
	}

	switch col.KindDetails.Kind {
	case typing.Date.Kind:
		parsedTime, err := typing.ParseDateFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse date: %w", err)
		}
		return parsedTime, nil

	case typing.Time.Kind:
		parsedTime, err := typing.ParseTimeFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse time: %w", err)
		}
		return parsedTime.Format(typing.PostgresTimeFormatNoTZ), nil

	case typing.TimestampNTZ.Kind:
		parsedTime, err := typing.ParseTimestampNTZFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp: %w", err)
		}
		return parsedTime, nil

	case typing.TimestampTZ.Kind:
		parsedTime, err := typing.ParseTimestampTZFromAny(value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse timestamp with timezone: %w", err)
		}
		return parsedTime, nil

	case typing.String.Kind:
		stringVal, err := typing.AssertType[string](value)
		if err != nil {
			// If it's not a string, try to marshal it to JSON
			if reflect.ValueOf(value).Kind() == reflect.Slice || reflect.ValueOf(value).Kind() == reflect.Map {
				jsonBytes, jsonErr := jsoniter.Marshal(value)
				if jsonErr != nil {
					return nil, fmt.Errorf("failed to marshal to JSON: %w", jsonErr)
				}
				return string(jsonBytes), nil
			}
			return fmt.Sprint(value), nil
		}
		return stringVal, nil

	case typing.Struct.Kind:
		if value == constants.ToastUnavailableValuePlaceholder {
			return fmt.Sprintf(`{"key":%q}`, constants.ToastUnavailableValuePlaceholder), nil
		}
		if reflect.TypeOf(value).Kind() == reflect.String {
			strVal := value.(string)
			if strVal == "" {
				return "{}", nil // Empty string should be empty JSON object
			}
			return strVal, nil
		}
		jsonBytes, err := jsoniter.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal struct to JSON: %w", err)
		}
		return string(jsonBytes), nil

	case typing.Array.Kind:
		// ClickHouse driver expects []string for Array(String) columns
		switch v := value.(type) {
		case []string:
			return v, nil
		case []any:
			result := make([]string, len(v))
			for i, elem := range v {
				if elem == nil {
					result[i] = ""
				} else {
					result[i] = fmt.Sprint(elem)
				}
			}
			return result, nil
		default:
			// Try to marshal and unmarshal as JSON array
			jsonBytes, err := jsoniter.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal array to JSON: %w", err)
			}
			var result []string
			if err := jsoniter.Unmarshal(jsonBytes, &result); err != nil {
				// If it can't be parsed as []string, return it as a single-element array
				return []string{string(jsonBytes)}, nil
			}
			return result, nil
		}

	case typing.Boolean.Kind:
		boolVal, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool, got %T", value)
		}
		return boolVal, nil

	case typing.Integer.Kind:
		switch v := value.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return v, nil
		case float64:
			return int64(v), nil
		case float32:
			return int64(v), nil
		case string:
			return v, nil // ClickHouse can parse strings to ints
		default:
			return value, nil
		}

	case typing.Float.Kind:
		switch v := value.(type) {
		case float32, float64:
			return v, nil
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return v, nil
		case string:
			return v, nil // ClickHouse can parse strings to floats
		default:
			return value, nil
		}

	case typing.EDecimal.Kind:
		if dec, ok := value.(*decimal.Decimal); ok {
			return dec.String(), nil
		}
		// Handle other numeric types
		switch v := value.(type) {
		case float64, float32, string:
			return v, nil
		default:
			return fmt.Sprint(value), nil
		}

	default:
		return value, nil
	}
}
