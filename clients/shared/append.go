package shared

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing/columns"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

func Append(ctx context.Context, dest destination.Destination, tableData *optimization.TableData, _ *webhooksclient.Client, opts types.AdditionalSettings) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := dest.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	tableConfig, err := dest.GetTableConfig(ctx, tableID, tableData.TopicConfig().DropDeletedColumns)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
	_, targetKeysMissing := columns.DiffAndFilter(
		tableData.ReadOnlyInMemoryCols().GetColumns(),
		tableConfig.GetColumns(),
		tableData.BuildColumnsToKeep(),
	)

	if tableConfig.CreateTable() {
		if err = CreateTable(ctx, dest, tableData.Mode(), tableConfig, opts.ColumnSettings, tableID, false, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	} else {
		if err = AlterTableAddColumns(ctx, dest, tableConfig, opts.ColumnSettings, tableID, targetKeysMissing); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	config := dest.GetConfig()
	if opts.UseTempTable && config.IsStagingTableReuseEnabled() {
		if stagingManager, ok := dest.(ReusableStagingTableManager); ok {
			return stagingManager.PrepareReusableStagingTable(
				ctx,
				tableData,
				tableConfig,
				dest.IdentifierFor(
					tableData.TopicConfig().BuildDatabaseAndSchemaPair(),
					GenerateReusableStagingTableName(
						tableID.Table(),
						config.GetStagingTableSuffix(),
					),
				).WithTemporaryTable(true),
				tableID,
				opts,
			)
		} else {
			return fmt.Errorf("destination %v does not support staging table reuse", dest)
		}
	} else {
		tempTableID := tableID
		if opts.UseTempTable {
			// Override tableID with tempTableID if we're using a temporary table
			tempTableID = opts.TempTableID
		}
		return dest.LoadDataIntoTable(
			ctx,
			tableData,
			tableConfig,
			tempTableID,
			tableID,
			opts,
			opts.UseTempTable,
		)
	}
}
