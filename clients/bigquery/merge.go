package bigquery

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/jitter"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"cloud.google.com/go/bigquery"

	"github.com/artie-labs/transfer/lib/destination/dml"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
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

func merge(ctx context.Context, tableData *optimization.TableData) ([]*Row, error) {
	var rows []*Row

	additionalDateFmts := config.FromContext(ctx).Config.SharedTransferConfig.AdditionalDateFormats
	for _, value := range tableData.RowsData() {
		data := make(map[string]bigquery.Value)
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(ctx, nil) {
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
func (s *Store) backfillColumn(ctx context.Context, column columns.Column, fqTableName string) error {
	if !column.ShouldBackfill() {
		// If we don't need to backfill, don't backfill.
		return nil
	}

	additionalDateFmts := config.FromContext(ctx).Config.SharedTransferConfig.AdditionalDateFormats

	defaultVal, err := column.DefaultValue(&columns.DefaultValueArgs{Escape: true, DestKind: s.Label()}, additionalDateFmts)
	if err != nil {
		return fmt.Errorf("failed to escape default value, err: %v", err)
	}

	escapedCol := column.Name(ctx, &sql.NameArgs{Escape: true, DestKind: s.Label()})
	query := fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`,
		// UPDATE table SET col = default_val WHERE col IS NULL
		fqTableName, escapedCol, defaultVal, escapedCol)

	logger.FromContext(ctx).WithFields(map[string]interface{}{
		"colName": column.RawName(),
		"query":   query,
		"table":   fqTableName,
	}).Info("backfilling column")
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

	tableConfig, err := s.getTableConfig(ctx, tableData)
	if err != nil {
		return err
	}

	log := logger.FromContext(ctx)
	// Check if all the columns exist in BigQuery
	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(),
		tableConfig.Columns(), tableData.TopicConfig.SoftDelete, tableData.TopicConfig.IncludeArtieUpdatedAt)

	fqName := tableData.ToFqName(ctx, s.Label(), true, s.projectID)
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: fqName,
		CreateTable: tableConfig.CreateTable(),
		ColumnOp:    constants.Add,
		CdcTime:     tableData.LatestCDCTs,
	}

	// Keys that exist in CDC stream, but not in BigQuery
	err = ddl.AlterTable(ctx, createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
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
	}

	err = ddl.AlterTable(ctx, deleteAlterTableArgs, srcKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	tableConfig.AuditColumnsToDelete(srcKeysMissing)

	// Infer the right data types from BigQuery before temp table creation.
	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)

	// Start temporary table creation
	tempAlterTableArgs := ddl.AlterTableArgs{
		Dwh:            s,
		Tc:             tableConfig,
		FqTableName:    fmt.Sprintf("%s_%s", tableData.ToFqName(ctx, s.Label(), false, s.projectID), tableData.TempTableSuffix()),
		CreateTable:    true,
		TemporaryTable: true,
		ColumnOp:       constants.Add,
	}

	if err = ddl.AlterTable(ctx, tempAlterTableArgs, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
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
			err = s.backfillColumn(ctx, col, fqName)
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
	rows, err := merge(ctx, tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	tableName := fmt.Sprintf("%s_%s", tableData.RawName(), tableData.TempTableSuffix())
	err = s.PutTable(ctx, tableData.TopicConfig.Database, tableName, rows)
	if err != nil {
		return fmt.Errorf("failed to insert into temp table: %s, error: %v", tableName, err)
	}

	var additionalEqualityStrings []string
	if tableData.TopicConfig.BigQueryPartitionSettings != nil {
		additionalDateFmts := config.FromContext(ctx).Config.SharedTransferConfig.AdditionalDateFormats
		distinctDates, err := tableData.DistinctDates(additionalDateFmts, tableData.TopicConfig.BigQueryPartitionSettings.PartitionField)
		if err != nil {
			return fmt.Errorf("failed to generate distinct dates, err: %v", err)
		}

		mergeString, err := tableData.TopicConfig.BigQueryPartitionSettings.GenerateMergeString(distinctDates)
		if err != nil {
			log.WithError(err).Warn("failed to generate merge string")
			return err
		}

		additionalEqualityStrings = []string{mergeString}
	}

	mergeQuery, err := dml.MergeStatement(ctx, &dml.MergeArgument{
		FqTableName:               fqName,
		AdditionalEqualityStrings: additionalEqualityStrings,
		SubQuery:                  tempAlterTableArgs.FqTableName,
		IdempotentKey:             tableData.TopicConfig.IdempotentKey,
		PrimaryKeys: tableData.PrimaryKeys(ctx, &sql.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}),
		ColumnsToTypes: *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:     tableData.TopicConfig.SoftDelete,
		DestKind:       s.Label(),
	})

	if err != nil {
		return err
	}

	_, err = s.Exec(mergeQuery)
	// This is above, in the case we have a head of line blocking because of an error
	// We will not create infinite temporary tables.
	_ = ddl.DropTemporaryTable(ctx, s, tempAlterTableArgs.FqTableName, false)
	if err != nil {
		return err
	}

	return nil
}
