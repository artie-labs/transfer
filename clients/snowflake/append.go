package snowflake

import (
	"context"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData) error {
	// TODO: Remove ctx.

	err := s.append(tableData)
	if IsAuthExpiredError(err) {
		slog.Warn("authentication has expired, will reload the Snowflake store and retry appending", slog.Any("err", err))
		s.reestablishConnection()
		return s.Append(ctx, tableData)
	}

	return err
}

func (s *Store) append(tableData *optimization.TableData) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	fqName := tableData.ToFqName(s.Label(), true, s.config.SharedDestinationConfig.UppercaseEscapedNames, "")
	tableConfig, err := s.getTableConfig(fqName, tableData.TopicConfig.DropDeletedColumns)
	if err != nil {
		return err
	}

	// We don't care about srcKeysMissing because we don't drop columns in append
	_, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig.SoftDelete, tableData.TopicConfig.IncludeArtieUpdatedAt,
		tableData.TopicConfig.IncludeDatabaseUpdatedAt, tableData.Mode())
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               s,
		Tc:                tableConfig,
		FqTableName:       fqName,
		CreateTable:       tableConfig.CreateTable(),
		ColumnOp:          constants.Add,
		CdcTime:           tableData.LatestCDCTs,
		UppercaseEscNames: &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	// Keys that exist in CDC stream, but not in Snowflake
	err = ddl.AlterTable(createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		slog.Warn("Failed to apply alter table", slog.Any("err", err))
		return err
	}

	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)
	return s.prepareTempTable(tableData, tableConfig, fqName)
}
