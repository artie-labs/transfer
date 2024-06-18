package shared

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func Append(dwh destination.DataWarehouse, tableData *optimization.TableData, opts types.AdditionalSettings) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableConfig, err := dwh.GetTableConfig(tableData)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
	_, targetKeysMissing := columns.Diff(
		tableData.ReadOnlyInMemoryCols(),
		tableConfig.Columns(),
		tableData.TopicConfig().SoftDelete,
		tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt,
		tableData.Mode(),
	)

	tableID := dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	createAlterTableArgs := ddl.AlterTableArgs{
		Dialect:     dwh.Dialect(),
		Tc:          tableConfig,
		TableID:     tableID,
		CreateTable: tableConfig.CreateTable(),
		ColumnOp:    constants.Add,
		CdcTime:     tableData.LatestCDCTs,
		Mode:        tableData.Mode(),
	}

	// Keys that exist in CDC stream, but not in DWH
	if err = createAlterTableArgs.AlterTable(dwh, targetKeysMissing...); err != nil {
		return fmt.Errorf("failed to alter table: %w", err)
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	if opts.UseTempTable {
		// Override tableID with tempTableID if we're using a temporary table
		tableID = opts.TempTableID
	}

	return dwh.PrepareTemporaryTable(
		tableData,
		tableConfig,
		tableID,
		opts,
		opts.UseTempTable,
	)
}
