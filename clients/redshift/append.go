package redshift

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableConfig, err := s.GetTableConfig(tableData)
	if err != nil {
		return err
	}

	// Check if all the columns exist in Redshift
	_, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig.SoftDelete, tableData.TopicConfig.IncludeArtieUpdatedAt, tableData.TopicConfig.IncludeDatabaseUpdatedAt, tableData.Mode())

	fqName := s.ToFullyQualifiedName(tableData, true)
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

	temporaryTableName := fmt.Sprintf("%s_%s", s.ToFullyQualifiedName(tableData, false), tableData.TempTableSuffix())
	if err = s.PrepareTemporaryTable(tableData, tableConfig, temporaryTableName, types.AdditionalSettings{}); err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	_, err = s.Exec(fmt.Sprintf(`ALTER TABLE %s APPEND FROM %s;`, fqName, temporaryTableName))
	return err
}
