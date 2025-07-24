package postgres

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

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

	return &stagingIterator{
		data: values,
		idx:  0,
	}, nil
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

	err = conn.Raw(func(driverConn any) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn() // conn is a *pgx.Conn
		cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
		stagingIterator, err := s.buildStagingIterator(tableData)
		if err != nil {
			return fmt.Errorf("failed to build staging iterator: %w", err)
		}

		copyCount, err := pgxConn.CopyFrom(ctx, pgxIdentifier, columns.ColumnNames(cols), stagingIterator)
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
	switch col.KindDetails.Kind {
	case typing.String.Kind:
		castedValue, err := typing.AssertType[string](value)
		if err != nil {
			return "", err
		}

		return castedValue, nil
	case typing.Integer.Kind:
		return value, nil
	case typing.Boolean.Kind:
		castedValue, err := typing.AssertType[bool](value)
		if err != nil {
			return "", err
		}

		return castedValue, nil
	default:
		return nil, fmt.Errorf("unsupported type %q, not implemented", col.KindDetails.Kind)
	}
}
