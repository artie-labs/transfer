package shared

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func Append(dwh destination.DataWarehouse, tableData *optimization.TableData, opts types.AppendOpts) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	tableConfig, err := dwh.GetTableConfig(tableData)
	if err != nil {
		return fmt.Errorf("failed to get table config: %w", err)
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
	_, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig().SoftDelete, tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt, tableData.Mode())

	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               dwh,
		Tc:                tableConfig,
		TableID:           tableID,
		CreateTable:       tableConfig.CreateTable(),
		ColumnOp:          constants.Add,
		CdcTime:           tableData.LatestCDCTs,
		UppercaseEscNames: ptr.ToBool(dwh.ShouldUppercaseEscapedNames()),
		Mode:              tableData.Mode(),
	}

	// Keys that exist in CDC stream, but not in DWH
	if err = createAlterTableArgs.AlterTable(targetKeysMissing...); err != nil {
		return fmt.Errorf("failed to alter table: %w", err)
	}

	if err = tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...); err != nil {
		return fmt.Errorf("failed to merge columns from destination: %w", err)
	}

	additionalSettings := types.AdditionalSettings{
		AdditionalCopyClause: opts.AdditionalCopyClause,
	}

	return dwh.PrepareTemporaryTable(tableData, tableConfig, opts.TempTableID, additionalSettings, false)
}
