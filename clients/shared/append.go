package shared

import (
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func Append(dwh destination.DataWarehouse, tableData *optimization.TableData, cfg config.Config, opts types.AppendOpts) error {
	if err := opts.Validate(); err != nil {
		return fmt.Errorf("failed to validate append options: %w", err)
	}

	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableID := dwh.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	fqName := tableID.FullyQualifiedName(dwh.ShouldUppercaseEscapedNames())
	tableConfig, err := dwh.GetTableConfig(tableData)
	if err != nil {
		return err
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
	_, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig().SoftDelete, tableData.TopicConfig().IncludeArtieUpdatedAt,
		tableData.TopicConfig().IncludeDatabaseUpdatedAt, tableData.Mode())

	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               dwh,
		Tc:                tableConfig,
		FqTableName:       fqName,
		CreateTable:       tableConfig.CreateTable(),
		ColumnOp:          constants.Add,
		CdcTime:           tableData.LatestCDCTs,
		UppercaseEscNames: &cfg.SharedDestinationConfig.UppercaseEscapedNames,
		Mode:              tableData.Mode(),
	}

	// Keys that exist in CDC stream, but not in DWH
	err = createAlterTableArgs.AlterTable(targetKeysMissing...)
	if err != nil {
		slog.Warn("Failed to apply alter table", slog.Any("err", err))
		return err
	}

	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)

	additionalSettings := types.AdditionalSettings{
		AdditionalCopyClause: opts.AdditionalCopyClause,
	}

	return dwh.PrepareTemporaryTable(tableData, tableConfig, opts.TempTableName, additionalSettings, false)
}
