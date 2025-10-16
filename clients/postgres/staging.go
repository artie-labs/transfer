package postgres

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/bigquery/converters"
	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// [stagingIterator] - This is an implementation of [pgx.CopyFromSource]
// Source: https://pkg.go.dev/github.com/jackc/pgx/v5#CopyFromSource
type stagingIterator struct {
	data [][]any
	idx  int
}

func (s *stagingIterator) Next() bool {
	return s.idx < len(s.data)
}

func (s *stagingIterator) Err() error {
	return nil
}

func (s *stagingIterator) Values() ([]any, error) {
	row := s.data[s.idx]
	s.idx++
	return row, nil
}

func (s *Store) buildStagingIterator(tableData *optimization.TableData) (pgx.CopyFromSource, error) {
	var values [][]any
	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	for _, row := range tableData.Rows() {
		var rowValues []any
		for _, col := range cols {
			value, _ := row.GetValue(col.Name())
			parsedValue, err := parseValue(value, col)
			if err != nil {
				return nil, fmt.Errorf("failed to parse value: %w", err)
			}

			rowValues = append(rowValues, parsedValue)
		}

		values = append(values, rowValues)
	}

	return &stagingIterator{data: values, idx: 0}, nil
}

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, opts.ColumnSettings, tempTableID); err != nil {
			return err
		}
	}

	castedTableID, ok := tempTableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast table identifier to dialect.TableIdentifier")
	}

	pgxIdentifier := []string{castedTableID.Schema(), castedTableID.Table()}
	conn, err := s.Store.Conn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get pgx conn: %w", err)
	}

	defer conn.Close()

	// This is lifted from pgx v5's docs: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
	err = conn.Raw(func(driverConn any) error {
		stdlibConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("failed to cast driverConn to *stdlib.Conn")
		}

		stagingIterator, err := s.buildStagingIterator(tableData)
		if err != nil {
			return fmt.Errorf("failed to build staging iterator: %w", err)
		}

		copyCount, err := stdlibConn.Conn().CopyFrom(ctx, pgxIdentifier, columns.ColumnNames(tableData.ReadOnlyInMemoryCols().ValidColumns()), stagingIterator)
		if err != nil {
			return fmt.Errorf("failed to copy from rows: %w", err)
		}

		if copyCount != int64(tableData.NumberOfRows()) {
			return fmt.Errorf("expected %d rows to be copied, but got %d", tableData.NumberOfRows(), copyCount)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to copy from rows: %w", err)
	}

	return nil
}

func parseValue(value any, col columns.Column) (any, error) {
	if value == nil {
		return value, nil
	}

	switch col.KindDetails.Kind {
	case typing.String.Kind:
		castedValue, err := typing.AssertType[string](value)
		if err != nil {
			return "", err
		}

		return castedValue, nil
	case typing.Integer.Kind:
		return converters.Int64Converter{}.Convert(value)
	case typing.Boolean.Kind:
		return converters.BooleanConverter{}.Convert(value)
	case typing.Struct.Kind:
		// If it's the toast placeholder value, wrap it in quotes so it's valid json
		if value == constants.ToastUnavailableValuePlaceholder {
			return fmt.Sprintf(`%q`, value), nil
		}
		return value, nil
	default:
		return value, nil
	}
}
