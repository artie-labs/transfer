package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/postgres"
	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

func TestDialect(ctx context.Context, store *postgres.Store, _dialect sql.Dialect) error {
	pgDialect, ok := _dialect.(dialect.PostgresDialect)
	if !ok {
		return fmt.Errorf("dialect is not a postgres dialect")
	}

	if err := testQuoteIdentifier(ctx, store, pgDialect); err != nil {
		return fmt.Errorf("failed to test quote identifier: %w", err)
	}

	// Test table
	if err := testTable(ctx, store, pgDialect); err != nil {
		return fmt.Errorf("failed to test table: %w", err)
	}

	// Test ADD COLUMN and DROP COLUMN
	if err := testAddDropColumn(ctx, store, pgDialect); err != nil {
		return fmt.Errorf("failed to test add/drop column: %w", err)
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

func testTable(ctx context.Context, store *postgres.Store, pgDialect dialect.PostgresDialect) error {
	testTableName := fmt.Sprintf("test_%s", strings.ToLower(stringutil.Random(5)))
	testTableID := store.IdentifierFor(kafkalib.DatabaseAndSchemaPair{Schema: "public"}, testTableName)
	if _, err := store.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s`, testTableName)); !pgDialect.IsTableDoesNotExistErr(err) {
		return fmt.Errorf("expected error when querying non-existent table, got nil")
	}

	// Now let's create the table and it should then exist.
	if _, err := store.ExecContext(ctx, pgDialect.BuildCreateTableQuery(testTableID, false, []string{"pk int PRIMARY KEY", "col text"})); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Now let's query the table and it should exist.
	if _, err := store.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s`, testTableName)); err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}
	{
		// Now let's insert some rows so we can test truncate.
		if _, err := store.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (pk, col) VALUES (1, 'test')`, testTableName)); err != nil {
			return fmt.Errorf("failed to insert row: %w", err)
		}

		rowCount, err := getRowCount(ctx, store, testTableName)
		if err != nil {
			return fmt.Errorf("failed to verify rows: %w", err)
		}

		if rowCount == 0 {
			return fmt.Errorf("expected some rows, got none")
		}
	}
	{
		// Now let's truncate the table and there shouldn't be any rows.
		if _, err := store.ExecContext(ctx, pgDialect.BuildTruncateTableQuery(testTableID)); err != nil {
			return fmt.Errorf("failed to truncate table: %w", err)
		}

		rowCount, err := getRowCount(ctx, store, testTableName)
		if err != nil {
			return fmt.Errorf("failed to verify rows: %w", err)
		}

		if rowCount != 0 {
			return fmt.Errorf("expected 0 rows, got %d", rowCount)
		}
	}
	{
		// Now let's drop the table and run this in a loop because it's idempotent.
		for range 3 {
			if _, err := store.ExecContext(ctx, pgDialect.BuildDropTableQuery(testTableID)); err != nil {
				return fmt.Errorf("failed to drop table: %w", err)
			}
		}

		// Now let's query and the table should not exist anymore.
		if _, err := store.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s`, testTableName)); !pgDialect.IsTableDoesNotExistErr(err) {
			return fmt.Errorf("expected table does not exist error, got %w", err)
		}
	}
	return nil
}

func getRowCount(ctx context.Context, store db.Store, testTableName string) (int, error) {
	var rowCount int
	if err := store.QueryRowContext(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM %s`, testTableName)).Scan(&rowCount); err != nil {
		return 0, fmt.Errorf("failed to query table: %w", err)
	}

	return rowCount, nil
}

func testAddDropColumn(ctx context.Context, store *postgres.Store, pgDialect dialect.PostgresDialect) error {
	testTableName := fmt.Sprintf("test_col_%s", strings.ToLower(stringutil.Random(5)))
	testTableID := store.IdentifierFor(kafkalib.DatabaseAndSchemaPair{Schema: "public"}, testTableName)

	// Create a test table first
	createTableSQL := pgDialect.BuildCreateTableQuery(testTableID, false, []string{"pk int PRIMARY KEY", "col1 text"})
	if _, err := store.ExecContext(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}

	colName := "new_column"

	// Test ADD COLUMN functionality
	if err := testAddColumn(ctx, store, pgDialect, testTableID, testTableName, colName); err != nil {
		return fmt.Errorf("failed to test add column: %w", err)
	}

	// Test DROP COLUMN functionality
	if err := testDropColumn(ctx, store, pgDialect, testTableID, testTableName, colName); err != nil {
		return fmt.Errorf("failed to test drop column: %w", err)
	}

	return nil
}

func testAddColumn(ctx context.Context, store *postgres.Store, pgDialect dialect.PostgresDialect, testTableID sql.TableIdentifier, testTableName string, colName string) error {
	sqlPart := fmt.Sprintf("%s varchar(100) DEFAULT 'test_value'", colName)
	// Add the column in a loop to test idempotency.
	for range 3 {
		if _, err := store.ExecContext(ctx, pgDialect.BuildAddColumnQuery(testTableID, sqlPart)); err != nil {
			return fmt.Errorf("failed to add column: %w", err)
		}
	}

	// Verify column exists by querying the table schema
	var columnExists bool
	if err := store.QueryRowContext(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = $1 AND column_name = $2 AND table_schema = 'public'
        )
    `, testTableName, colName).Scan(&columnExists); err != nil {
		return fmt.Errorf("failed to check if column exists: %w", err)
	}

	if !columnExists {
		return fmt.Errorf("expected column %q to exist, got none", colName)
	}

	return nil
}

func testDropColumn(ctx context.Context, store *postgres.Store, pgDialect dialect.PostgresDialect, testTableID sql.TableIdentifier, testTableName string, colName string) error {
	// Drop column in a loop to test idempotency.
	for range 3 {
		if _, err := store.ExecContext(ctx, pgDialect.BuildDropColumnQuery(testTableID, colName)); err != nil {
			return fmt.Errorf("failed to drop column: %w", err)
		}
	}

	// Verify column no longer exists
	var columnExists bool
	if err := store.QueryRowContext(ctx, `
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = $1 AND column_name = $2 AND table_schema = 'public'
        )
    `, testTableName, colName).Scan(&columnExists); err != nil {
		return fmt.Errorf("failed to check if column exists: %w", err)
	}

	if columnExists {
		return fmt.Errorf("expected column %q to be dropped, got none", colName)
	}

	return nil
}
