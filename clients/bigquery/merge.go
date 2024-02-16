package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/artie-labs/transfer/clients/utils"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/dml"
	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type Row struct {
	data map[string]bigquery.Value
}

func NewRow(data map[string]bigquery.Value) *Row {
	return &Row{
		data: data,
	}
}

func (r *Row) Save() (map[string]bigquery.Value, string, error) {
	return r.data, bigquery.NoDedupeID, nil
}

func (s *Store) merge(tableData *optimization.TableData) ([]*Row, error) {
	var rows []*Row

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	for _, value := range tableData.Rows() {
		data := make(map[string]bigquery.Value)
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.config.SharedDestinationConfig.UppercaseEscapedNames, nil) {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal, err := castColVal(value[col], colKind, additionalDateFmts)
			if err != nil {
				return nil, fmt.Errorf("failed to cast col %s: %w", col, err)
			}

			if colVal != nil {
				data[col] = colVal
			}
		}

		rows = append(rows, NewRow(data))
	}

	return rows, nil
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	// TODO - write test for this.
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	tableConfig, err := s.getTableConfig(tableData)
	if err != nil {
		return err
	}

	// Check if all the columns exist in BigQuery
	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(),
		tableConfig.Columns(), tableData.TopicConfig.SoftDelete,
		tableData.TopicConfig.IncludeArtieUpdatedAt, tableData.TopicConfig.IncludeDatabaseUpdatedAt, tableData.Mode())

	fqName := tableData.ToFqName(s.Label(), true, s.config.SharedDestinationConfig.UppercaseEscapedNames, s.config.BigQuery.ProjectID)
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               s,
		Tc:                tableConfig,
		FqTableName:       fqName,
		CreateTable:       tableConfig.CreateTable(),
		ColumnOp:          constants.Add,
		CdcTime:           tableData.LatestCDCTs,
		UppercaseEscNames: &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	// Keys that exist in CDC stream, but not in BigQuery
	err = ddl.AlterTable(createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		slog.Warn("Failed to apply alter table", slog.Any("err", err))
		return err
	}

	// Keys that exist in BigQuery, but don't exist in our CDC stream.
	// createTable is set to false because table creation requires a column to be added
	// Which means, we'll only do it upon Add columns.
	deleteAlterTableArgs := ddl.AlterTableArgs{
		Dwh:                    s,
		Tc:                     tableConfig,
		FqTableName:            fqName,
		CreateTable:            false,
		ColumnOp:               constants.Delete,
		ContainOtherOperations: tableData.ContainOtherOperations(),
		CdcTime:                tableData.LatestCDCTs,
		UppercaseEscNames:      &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	err = ddl.AlterTable(deleteAlterTableArgs, srcKeysMissing...)
	if err != nil {
		slog.Warn("Failed to apply alter table", slog.Any("err", err))
		return err
	}

	tableConfig.AuditColumnsToDelete(srcKeysMissing)

	// Infer the right data types from BigQuery before temp table creation.
	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)

	// Start temporary table creation
	tempAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               s,
		Tc:                tableConfig,
		FqTableName:       fmt.Sprintf("%s_%s", tableData.ToFqName(s.Label(), false, s.config.SharedDestinationConfig.UppercaseEscapedNames, s.config.BigQuery.ProjectID), tableData.TempTableSuffix()),
		CreateTable:       true,
		TemporaryTable:    true,
		ColumnOp:          constants.Add,
		UppercaseEscNames: &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	if err = ddl.AlterTable(tempAlterTableArgs, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
		return fmt.Errorf("failed to create temp table: %w", err)
	}
	// End temporary table creation

	// Backfill columns if necessary
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		var attempts int
		for {
			err = utils.BackfillColumn(s.config, s, col, fqName)
			if err == nil {
				tableConfig.Columns().UpsertColumn(col.RawName(), columns.UpsertColumnArg{
					Backfilled: ptr.ToBool(true),
				})
				break
			}

			if TableUpdateQuotaErr(err) {
				err = nil
				attempts += 1
				time.Sleep(jitter.Jitter(1500, jitter.DefaultMaxMs, attempts))
			} else {
				return fmt.Errorf("failed to backfill col: %v, default value: %v, err: %w", col.RawName(), col.RawDefaultValue(), err)
			}
		}

	}

	// Perform actual merge now
	rows, err := s.merge(tableData)
	if err != nil {
		slog.Warn("Failed to generate the merge query", slog.Any("err", err))
		return err
	}

	tableName := fmt.Sprintf("%s_%s", tableData.RawName(), tableData.TempTableSuffix())
	err = s.PutTable(ctx, tableData.TopicConfig.Database, tableName, rows)
	if err != nil {
		return fmt.Errorf("failed to insert into temp table %s: %w", tableName, err)
	}

	defer func() {
		// Regardless of outcome, drop the temporary table once this function is returning.
		if dropErr := ddl.DropTemporaryTable(s, tempAlterTableArgs.FqTableName, false); dropErr != nil {
			slog.Warn("Failed to drop temporary table", slog.Any("err", dropErr), slog.String("tableName", tempAlterTableArgs.FqTableName))
		}
	}()

	var additionalEqualityStrings []string
	if tableData.TopicConfig.BigQueryPartitionSettings != nil {
		additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
		distinctDates, err := tableData.DistinctDates(tableData.TopicConfig.BigQueryPartitionSettings.PartitionField, additionalDateFmts)
		if err != nil {
			return fmt.Errorf("failed to generate distinct dates: %w", err)
		}

		mergeString, err := tableData.TopicConfig.BigQueryPartitionSettings.GenerateMergeString(distinctDates)
		if err != nil {
			slog.Warn("Failed to generate merge string", slog.Any("err", err))
			return err
		}

		additionalEqualityStrings = []string{mergeString}
	}

	mergeArg := dml.MergeArgument{
		FqTableName:               fqName,
		AdditionalEqualityStrings: additionalEqualityStrings,
		SubQuery:                  tempAlterTableArgs.FqTableName,
		IdempotentKey:             tableData.TopicConfig.IdempotentKey,
		PrimaryKeys:               tableData.PrimaryKeys(s.config.SharedDestinationConfig.UppercaseEscapedNames, &sql.NameArgs{Escape: true, DestKind: s.Label()}),
		ColumnsToTypes:            *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:                tableData.TopicConfig.SoftDelete,
		DestKind:                  s.Label(),
		UppercaseEscNames:         &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	mergeQuery, err := mergeArg.GetStatement()
	if err != nil {
		return err
	}

	_, err = s.Exec(mergeQuery)
	return err
}
