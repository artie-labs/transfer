package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cloud.google.com/go/bigquery"

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
	for _, value := range tableData.RowsData() {
		data := make(map[string]bigquery.Value)
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.config.SharedDestinationConfig.UppercaseEscapedNames, nil) {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal, err := castColVal(value[col], colKind, additionalDateFmts)
			if err != nil {
				return nil, fmt.Errorf("failed to cast col: %v, err: %v", col, err)
			}

			if colVal != nil {
				data[col] = colVal
			}
		}

		rows = append(rows, NewRow(data))
	}

	return rows, nil
}

// BackfillColumn will perform a backfill to the destination and also update the comment within a transaction.
// Source: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#column_set_options_list
func (s *Store) backfillColumn(column columns.Column, fqTableName string) error {
	if !column.ShouldBackfill() {
		// If we don't need to backfill, don't backfill.
		return nil
	}

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats

	defaultVal, err := column.DefaultValue(&columns.DefaultValueArgs{Escape: true, DestKind: s.Label()}, additionalDateFmts)
	if err != nil {
		return fmt.Errorf("failed to escape default value, err: %v", err)
	}

	escapedCol := column.Name(s.config.SharedDestinationConfig.UppercaseEscapedNames, &sql.NameArgs{Escape: true, DestKind: s.Label()})
	query := fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`,
		// UPDATE table SET col = default_val WHERE col IS NULL
		fqTableName, escapedCol, defaultVal, escapedCol)

	slog.Info(
		"backfilling column",
		slog.String("colName", column.RawName()),
		slog.String("query", query),
		slog.String("table", fqTableName),
	)
	_, err = s.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to backfill, err: %v, query: %v", err, query)
	}

	query = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET OPTIONS (description=`%s`);",
		// ALTER TABLE table ALTER COLUMN col set OPTIONS (description=...)
		fqTableName, escapedCol, `{"backfilled": true}`,
	)
	_, err = s.Exec(query)
	return err
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	// TODO - write test for this.
	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
		// There's no rows or columns. Let's skip.
		return nil
	}

	tableConfig, err := s.getTableConfig(tableData)
	if err != nil {
		return err
	}

	// Check if all the columns exist in BigQuery
	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(),
		tableConfig.Columns(), tableData.TopicConfig.SoftDelete,
		tableData.TopicConfig.IncludeArtieUpdatedAt, tableData.TopicConfig.IncludeDatabaseUpdatedAt)

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
		return fmt.Errorf("failed to create temp table, error: %v", err)
	}
	// End temporary table creation

	// Backfill columns if necessary
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		var attempts int
		for {
			err = s.backfillColumn(col, fqName)
			if err == nil {
				tableConfig.Columns().UpsertColumn(col.RawName(), columns.UpsertColumnArg{
					Backfilled: ptr.ToBool(true),
				})
				break
			}

			if TableUpdateQuotaErr(err) {
				err = nil
				attempts += 1
				time.Sleep(time.Duration(jitter.JitterMs(1500, attempts)) * time.Millisecond)
			} else {
				return fmt.Errorf("failed to backfill col: %v, default value: %v, err: %v", col.RawName(), col.RawDefaultValue(), err)
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
		return fmt.Errorf("failed to insert into temp table: %s, error: %v", tableName, err)
	}

	var additionalEqualityStrings []string
	if tableData.TopicConfig.BigQueryPartitionSettings != nil {
		additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
		distinctDates, err := tableData.DistinctDates(tableData.TopicConfig.BigQueryPartitionSettings.PartitionField, additionalDateFmts)
		if err != nil {
			return fmt.Errorf("failed to generate distinct dates, err: %v", err)
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
	// This is above, in the case we have a head of line blocking because of an error
	// We will not create infinite temporary tables.
	_ = ddl.DropTemporaryTable(s, tempAlterTableArgs.FqTableName, false)
	if err != nil {
		return err
	}

	return nil
}
