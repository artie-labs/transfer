package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

func TestDialect(ctx context.Context, store db.Store, _dialect sql.Dialect) error {
	pgDialect, ok := _dialect.(dialect.PostgresDialect)
	if !ok {
		return fmt.Errorf("dialect is not a postgres dialect")
	}

	if err := testQuoteIdentifier(ctx, store, pgDialect); err != nil {
		return fmt.Errorf("failed to test quote identifier: %w", err)
	}

	// Try to query for a table and it doesn't exist.
	_, err := store.QueryContext(ctx, `SELECT * FROM non_existent_table`)
	if err == nil {
		return fmt.Errorf("expected error when querying non-existent table, got nil")
	}

	if !pgDialect.IsTableDoesNotExistErr(err) {
		return fmt.Errorf("expected table does not exist error, got %v", err)
	}

	return nil
}

func testQuoteIdentifier(ctx context.Context, store db.Store, pgDialect dialect.PostgresDialect) error {
	expectedRows := 5
	// Test quote identifiers.
	testTableName := fmt.Sprintf("test_%s", strings.ToLower(stringutil.Random(5)))
	if _, err := store.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (pk int PRIMARY KEY, "EscapedCol" text)`, testTableName)); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Now let's insert some rows
	for i := range expectedRows {
		if _, err := store.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (pk, "EscapedCol") VALUES (%d, 'test')`, testTableName, i)); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}
	}

	sqlRows, err := store.QueryContext(ctx, fmt.Sprintf(`SELECT pk, %s FROM %s`, pgDialect.QuoteIdentifier("EscapedCol"), testTableName))
	if err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}

	rows, err := sql.RowsToObjects(sqlRows)
	if err != nil {
		return fmt.Errorf("failed to convert rows to objects: %w", err)
	}

	var expectedValues []any
	for _, row := range rows {
		value, ok := row["EscapedCol"]
		if !ok {
			return fmt.Errorf("expected value for EscapedCol, got %v", row)
		}

		expectedValues = append(expectedValues, value)
	}

	if len(expectedValues) != expectedRows {
		return fmt.Errorf("expected %d rows, got %d", expectedRows, len(expectedValues))
	}

	return nil
}
