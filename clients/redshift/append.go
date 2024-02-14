package redshift

import (
	"context"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableConfig, err := s.getTableConfig(tableData)
	if err != nil {
		return err
	}

	fqName := tableData.ToFqName(s.Label(), true, s.config.SharedDestinationConfig.UppercaseEscapedNames, "")
	// Check if all the columns exist in Redshift
	_, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig.SoftDelete, tableData.TopicConfig.IncludeArtieUpdatedAt, tableData.TopicConfig.IncludeDatabaseUpdatedAt, tableData.Mode())

	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               s,
		Tc:                tableConfig,
		FqTableName:       fqName,
		CreateTable:       tableConfig.CreateTable(),
		ColumnOp:          constants.Add,
		CdcTime:           tableData.LatestCDCTs,
		UppercaseEscNames: &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	// Keys that exist in CDC stream, but not in Redshift
	err = ddl.AlterTable(createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		slog.Warn("Failed to apply alter table", slog.Any("err", err))
		return err
	}

	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)
	return s.prepareTempTable(ctx, tableData, tableConfig, fqName)
}
