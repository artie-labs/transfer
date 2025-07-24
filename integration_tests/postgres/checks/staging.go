package checks

import (
	"context"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/clients/postgres"
	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestStagingTable(ctx context.Context, store *postgres.Store) error {
	tableID := dialect.NewTableIdentifier("public", fmt.Sprintf("test_%s", strings.ToLower(stringutil.Random(5))))

	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))

	if err := cols.UpsertColumn("id", columns.UpsertColumnArg{PrimaryKey: typing.ToPtr(true)}); err != nil {
		return fmt.Errorf("failed to upsert column: %w", err)
	}

	expectedRows := 10_000
	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, kafkalib.TopicConfig{}, tableID.Table())
	for i := range expectedRows {
		tableData.InsertRow(fmt.Sprintf("%d", i), map[string]any{"id": i, "name": fmt.Sprintf("name_%d", i)}, false)
	}

	tc := types.NewDestinationTableConfig(cols.GetColumns(), false)
	if err := store.PrepareTemporaryTable(ctx, tableData, tc, tableID, tableID, types.AdditionalSettings{}, true); err != nil {
		return fmt.Errorf("failed to prepare temporary table: %w", err)
	}

	rowCount, err := getRowCount(ctx, store, tableID.Table())
	if err != nil {
		return fmt.Errorf("failed to get row count: %w", err)
	}

	if rowCount != expectedRows {
		return fmt.Errorf("expected %d rows, got %d", expectedRows, rowCount)
	}

	return nil
}
