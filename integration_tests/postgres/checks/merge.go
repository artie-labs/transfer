package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/postgres"
	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestMergeOperations(ctx context.Context, store *postgres.Store) error {
	if err := testSoftDeleteMerge(ctx, store); err != nil {
		return fmt.Errorf("failed soft delete merge test: %w", err)
	}

	if err := testRegularMerge(ctx, store); err != nil {
		return fmt.Errorf("failed regular merge test: %w", err)
	}

	return nil
}

func testSoftDeleteMerge(ctx context.Context, store *postgres.Store) error {
	tableID := dialect.NewTableIdentifier("public", fmt.Sprintf("test_merge_soft_%s", strings.ToLower(stringutil.Random(5))))

	cols := columns.NewColumns(nil)
	cols.AddColumn(columns.NewColumn("id", typing.BuildIntegerKind(typing.IntegerKind)))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))
	cols.AddColumn(columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean))

	if err := cols.UpsertColumn("id", columns.UpsertColumnArg{PrimaryKey: typing.ToPtr(true)}); err != nil {
		return fmt.Errorf("failed to upsert column: %w", err)
	}

	tableData := optimization.NewTableData(cols, config.Replication, []string{"id"}, kafkalib.TopicConfig{Schema: "public", SoftDelete: true}, tableID.Table())

	// Insert initial data
	for i := 1; i <= 5; i++ {
		tableData.InsertRow(fmt.Sprintf("%d", i), map[string]any{
			"id":                                i,
			"name":                              fmt.Sprintf("user_%d", i),
			constants.DeleteColumnMarker:        false,
			constants.OnlySetDeleteColumnMarker: false,
		}, false)
	}

	if _, err := store.Merge(ctx, tableData, nil); err != nil {
		return fmt.Errorf("failed initial merge: %w", err)
	}

	if err := verifyRowCount(ctx, store, tableID.Table(), 5); err != nil {
		return fmt.Errorf("failed initial count verification: %w", err)
	}

	tableData.WipeData()

	// Test soft delete (only sets delete marker)
	tableData.InsertRow("3", map[string]any{
		"id":                                3,
		"name":                              "user_3",
		constants.DeleteColumnMarker:        true,
		constants.OnlySetDeleteColumnMarker: true,
	}, true)

	if _, err := store.Merge(ctx, tableData, nil); err != nil {
		return fmt.Errorf("failed soft delete merge: %w", err)
	}

	// Should still have 5 rows but one marked as deleted
	if err := verifyRowCount(ctx, store, tableID.Table(), 5); err != nil {
		return fmt.Errorf("failed soft delete count verification: %w", err)
	}

	// Verify the delete marker was set
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE id = 3`,
		store.Dialect().QuoteIdentifier(constants.DeleteColumnMarker), tableID.Table())
	rows, err := store.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query delete marker: %w", err)
	}

	if !rows.Next() {
		return fmt.Errorf("expected row with id=3")
	}

	var deleteMarker bool
	if err := rows.Scan(&deleteMarker); err != nil {
		return fmt.Errorf("failed to scan delete marker: %w", err)
	}

	if !deleteMarker {
		return fmt.Errorf("expected delete marker to be true")
	}

	return store.DropTable(ctx, tableID.WithTemporaryTable(true))
}

func testRegularMerge(ctx context.Context, store *postgres.Store) error {
	tableID := dialect.NewTableIdentifier("public", fmt.Sprintf("test_merge_reg_%s", strings.ToLower(stringutil.Random(5))))

	cols := columns.NewColumns(nil)
	cols.AddColumn(columns.NewColumn("id", typing.BuildIntegerKind(typing.IntegerKind)))
	cols.AddColumn(columns.NewColumn("value", typing.String))
	cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))
	cols.AddColumn(columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean))

	if err := cols.UpsertColumn("id", columns.UpsertColumnArg{PrimaryKey: typing.ToPtr(true)}); err != nil {
		return fmt.Errorf("failed to upsert column: %w", err)
	}

	tableData := optimization.NewTableData(cols, config.Replication, []string{"id"}, kafkalib.TopicConfig{Schema: "public", SoftDelete: false}, tableID.Table())

	// Insert initial data
	for i := 1; i <= 10; i++ {
		tableData.InsertRow(fmt.Sprintf("%d", i), map[string]any{
			"id":                                i,
			"value":                             fmt.Sprintf("initial_%d", i),
			constants.DeleteColumnMarker:        false,
			constants.OnlySetDeleteColumnMarker: false,
		}, false)
	}

	if _, err := store.Merge(ctx, tableData, nil); err != nil {
		return fmt.Errorf("failed initial merge: %w", err)
	}

	if err := verifyRowCount(ctx, store, tableID.Table(), 10); err != nil {
		return fmt.Errorf("failed initial count verification: %w", err)
	}

	tableData.WipeData()

	// Test updates and deletes
	// Update records 1-3
	for i := 1; i <= 3; i++ {
		tableData.InsertRow(fmt.Sprintf("%d", i), map[string]any{
			"id":                                i,
			"value":                             fmt.Sprintf("updated_%d", i),
			constants.DeleteColumnMarker:        false,
			constants.OnlySetDeleteColumnMarker: false,
		}, false)
	}

	// Delete records 8-10
	for i := 8; i <= 10; i++ {
		tableData.InsertRow(fmt.Sprintf("%d", i), map[string]any{
			"id":                                i,
			"value":                             fmt.Sprintf("initial_%d", i),
			constants.DeleteColumnMarker:        true,
			constants.OnlySetDeleteColumnMarker: false,
		}, true)
	}

	if _, err := store.Merge(ctx, tableData, nil); err != nil {
		return fmt.Errorf("failed update/delete merge: %w", err)
	}

	// Should have 7 rows left (10 - 3 deleted)
	if err := verifyRowCount(ctx, store, tableID.Table(), 7); err != nil {
		return fmt.Errorf("failed final count verification: %w", err)
	}

	// Verify updates worked
	query := fmt.Sprintf(`SELECT value FROM %s WHERE id = 1`, tableID.Table())
	rows, err := store.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query updated value: %w", err)
	}

	if !rows.Next() {
		return fmt.Errorf("expected row with id=1")
	}

	var value string
	if err := rows.Scan(&value); err != nil {
		return fmt.Errorf("failed to scan value: %w", err)
	}

	if value != "updated_1" {
		return fmt.Errorf("expected 'updated_1', got %q", value)
	}

	return store.DropTable(ctx, tableID.WithTemporaryTable(true))
}

func verifyRowCount(ctx context.Context, store *postgres.Store, tableName string, expected int) error {
	count, err := getRowCount(ctx, store, tableName)
	if err != nil {
		return err
	}

	if count != expected {
		return fmt.Errorf("expected %d rows, got %d", expected, count)
	}

	return nil
}
