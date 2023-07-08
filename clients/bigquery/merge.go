package bigquery

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/jitter"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"cloud.google.com/go/bigquery"

	"github.com/artie-labs/transfer/lib/dwh/dml"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
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

func merge(tableData *optimization.TableData) ([]*Row, error) {
	var rows []*Row
	for _, value := range tableData.RowsData() {
		data := make(map[string]bigquery.Value)
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(nil) {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal, err := CastColVal(value[col], colKind)
			if err != nil {
				return nil, err
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

	defaultVal, err := column.DefaultValue(&columns.DefaultValueArgs{
		Escape:   true,
		DestKind: s.Label(),
	})

	if err != nil {
		return fmt.Errorf("failed to escape default value, err: %v", err)
	}

	fqTableName = strings.ToLower(fqTableName)
	escapedCol := column.Name(&columns.NameArgs{Escape: true, DestKind: s.Label()})
	query := fmt.Sprintf(`UPDATE %s SET %s = %v WHERE %s IS NULL;`,
		// UPDATE table SET col = default_val WHERE col IS NULL
		fqTableName, escapedCol, defaultVal, escapedCol)

	logger.FromContext(ctx).WithFields(map[string]interface{}{
		"colName": column.Name(nil),
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
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: tableData.ToFqName(ctx, s.Label()),
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
		FqTableName:            tableData.ToFqName(ctx, s.Label()),
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

	// Make sure we are still trying to delete it.
	// If not, then we should assume the column is good and then remove it from our in-mem store.
	for colToDelete := range tableConfig.ReadOnlyColumnsToDelete() {
		var found bool
		for _, col := range srcKeysMissing {
			if found = col.Name(nil) == colToDelete; found {
				// Found it.
				break
			}
		}

		if !found {
			// Only if it is NOT found shall we try to delete from in-memory (because we caught up)
			tableConfig.ClearColumnsToDeleteByColName(colToDelete)
		}
	}

	// Infer the right data types from BigQuery before temp table creation.
	tableData.UpdateInMemoryColumnsFromDestination(tableConfig.Columns().GetColumns()...)

	// Start temporary table creation
	tempAlterTableArgs := ddl.AlterTableArgs{
		Dwh:            s,
		Tc:             tableConfig,
		FqTableName:    fmt.Sprintf("%s_%s", tableData.ToFqName(ctx, s.Label()), tableData.TempTableSuffix()),
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
			err = s.backfillColumn(ctx, col, tableData.ToFqName(ctx, s.Label()))
			if err == nil {
				tableConfig.Columns().UpsertColumn(col.Name(nil), columns.UpsertColumnArg{
					Backfilled: ptr.ToBool(true),
				})
				break
			}

			if TableUpdateQuotaErr(err) {
				err = nil
				attempts += 1
				time.Sleep(time.Duration(jitter.JitterMs(1500, attempts)) * time.Millisecond)
			} else {
				defaultVal, _ := col.DefaultValue(nil)
				return fmt.Errorf("failed to backfill col: %v, default value: %v, err: %v",
					col.Name(nil), defaultVal, err)
			}
		}

	}

	// Perform actual merge now
	rows, err := merge(tableData)
	if err != nil {
		log.WithError(err).Warn("failed to generate the merge query")
		return err
	}

	tableName := fmt.Sprintf("%s_%s", tableData.Name(), tableData.TempTableSuffix())
	err = s.PutTable(ctx, tableData.TopicConfig.Database, tableName, rows)
	if err != nil {
		return fmt.Errorf("failed to insert into temp table: %s, error: %v", tableName, err)
	}

	mergeQuery, err := dml.MergeStatement(&dml.MergeArgument{
		FqTableName:   tableData.ToFqName(ctx, constants.BigQuery),
		SubQuery:      tempAlterTableArgs.FqTableName,
		IdempotentKey: tableData.TopicConfig.IdempotentKey,
		PrimaryKeys: tableData.PrimaryKeys(&columns.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}),
		Columns: tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(&columns.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}),
		ColumnsToTypes: *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:     tableData.TopicConfig.SoftDelete,
		BigQuery:       true,
	})

	if err != nil {
		return err
	}

	_, err = s.Exec(mergeQuery)
	if err != nil {
		return err
	}

	_ = ddl.DropTemporaryTable(ctx, s, tempAlterTableArgs.FqTableName, false)
	return nil
}
