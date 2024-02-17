package snowflake

import (
	"log/slog"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) Append(tableData *optimization.TableData) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = s.append(tableData)
		if IsAuthExpiredError(err) {
			slog.Warn("Authentication has expired, will reload the Snowflake store and retry appending", slog.Any("err", err))
			s.reestablishConnection()
		} else {
			break
		}
	}

	return err
}

func (s *Store) append(tableData *optimization.TableData) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	fqName := s.ToFullyQualifiedName(tableData, true)
	tableConfig, err := s.GetTableConfig(tableData)
	if err != nil {
		return err
	}

	// We don't care about srcKeysMissing because we don't drop columns when we append.
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

	// TODO: For history mode - in the future, we could also have a separate stage name for history mode so we can enable parallel processing.
	return s.PrepareTemporaryTable(tableData, tableConfig, fqName, types.AdditionalSettings{
		AdditionalCopyClause: `FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER= '\t' FIELD_OPTIONALLY_ENCLOSED_BY='"' NULL_IF='\\N' EMPTY_FIELD_AS_NULL=FALSE) PURGE = TRUE`,
	})
}
