package snowflake

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/clients/utils"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/dml"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
)

// prepareTempTable does the following:
// 1) Create the temporary table
// 2) Load in-memory table -> CSV
// 3) Runs PUT to upload CSV to Snowflake staging (auto-compression with GZIP)
// 4) Runs COPY INTO with the columns specified into temporary table
// 5) Deletes CSV generated from (2)
func (s *Store) prepareTempTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string) error {
	tempAlterTableArgs := ddl.AlterTableArgs{
		Dwh:            s,
		Tc:             tableConfig,
		FqTableName:    tempTableName,
		CreateTable:    true,
		TemporaryTable: true,
		ColumnOp:       constants.Add,
	}

	if err := ddl.AlterTable(ctx, tempAlterTableArgs, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
		return fmt.Errorf("failed to create temp table, error: %v", err)
	}

	fp, err := s.loadTemporaryTable(ctx, tableData, tempTableName)
	if err != nil {
		return fmt.Errorf("failed to load temporary table, err: %v", err)
	}

	if _, err = s.Exec(fmt.Sprintf("PUT file://%s @%s AUTO_COMPRESS=TRUE", fp, addPrefixToTableName(tempTableName, "%"))); err != nil {
		return fmt.Errorf("failed to run PUT for temporary table, err: %v", err)
	}

	_, err = s.Exec(fmt.Sprintf("COPY INTO %s (%s) FROM (SELECT %s FROM @%s)",
		// Copy into temporary tables (column ...)
		tempTableName, strings.Join(tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(ctx, &sql.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}), ","),
		// Escaped columns, TABLE NAME
		escapeColumns(tableData.ReadOnlyInMemoryCols(), ","), addPrefixToTableName(tempTableName, "%")))

	if err != nil {
		return fmt.Errorf("failed to load staging file into temporary table, err: %v", err)
	}

	if deleteErr := os.RemoveAll(fp); deleteErr != nil {
		logger.FromContext(ctx).WithError(deleteErr).WithField("filePath", fp).Warn("failed to delete temp file")
	}

	return nil
}

// loadTemporaryTable will write the data into /tmp/newTableName.csv
// This way, another function can call this and then invoke a Snowflake PUT.
// Returns the file path and potential error
func (s *Store) loadTemporaryTable(ctx context.Context, tableData *optimization.TableData, newTableName string) (string, error) {
	filePath := fmt.Sprintf("/tmp/%s.csv", newTableName)
	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = '\t'
	for _, value := range tableData.RowsData() {
		var row []string
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(ctx, nil) {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal := value[col]
			// Check
			castedValue, castErr := castColValStaging(ctx, colVal, colKind)
			if castErr != nil {
				return "", castErr
			}

			row = append(row, castedValue)
		}

		if err = writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write to csv, err: %v", err)
		}
	}

	writer.Flush()
	return filePath, writer.Error()
}

func (s *Store) mergeWithStages(ctx context.Context, tableData *optimization.TableData) error {
	// TODO - better test coverage for `mergeWithStages`
	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
		// There's no rows. Let's skip.
		return nil
	}

	fqName := tableData.ToFqName(ctx, constants.Snowflake, true)
	tableConfig, err := s.getTableConfig(ctx, fqName, tableData.TopicConfig.DropDeletedColumns)
	if err != nil {
		return err
	}

	log := logger.FromContext(ctx)
	// Check if all the columns exist in Snowflake
	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
		tableData.TopicConfig.SoftDelete, tableData.TopicConfig.IncludeArtieUpdatedAt)
	createAlterTableArgs := ddl.AlterTableArgs{
		Dwh:         s,
		Tc:          tableConfig,
		FqTableName: fqName,
		CreateTable: tableConfig.CreateTable(),
		ColumnOp:    constants.Add,
		CdcTime:     tableData.LatestCDCTs,
	}

	// Keys that exist in CDC stream, but not in Snowflake
	err = ddl.AlterTable(ctx, createAlterTableArgs, targetKeysMissing...)
	if err != nil {
		log.WithError(err).Warn("failed to apply alter table")
		return err
	}

	// Keys that exist in Snowflake, but don't exist in our CDC stream.
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
	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)
	temporaryTableName := fmt.Sprintf("%s_%s", tableData.ToFqName(ctx, s.Label(), false), tableData.TempTableSuffix())
	if err = s.prepareTempTable(ctx, tableData, tableConfig, temporaryTableName); err != nil {
		return err
	}

	// Now iterate over all the in-memory cols and see which one requires backfill.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		err = utils.BackfillColumn(ctx, s, col, tableData.ToFqName(ctx, s.Label(), true))
		if err != nil {
			return fmt.Errorf("failed to backfill col: %v, default value: %v, err: %v", col.RawName(), col.RawDefaultValue(), err)
		}

		tableConfig.Columns().UpsertColumn(col.RawName(), columns.UpsertColumnArg{
			Backfilled: ptr.ToBool(true),
		})
	}

	// Prepare merge statement
	mergeQuery, err := dml.MergeStatement(ctx, &dml.MergeArgument{
		FqTableName:   tableData.ToFqName(ctx, constants.Snowflake, true),
		SubQuery:      temporaryTableName,
		IdempotentKey: tableData.TopicConfig.IdempotentKey,
		PrimaryKeys: tableData.PrimaryKeys(ctx, &sql.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}),
		ColumnsToTypes: *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:     tableData.TopicConfig.SoftDelete,
	})

	if err != nil {
		return fmt.Errorf("failed to generate merge statement, err: %v", err)
	}

	log.WithField("query", mergeQuery).Debug("executing...")
	_, err = s.Exec(mergeQuery)
	if err != nil {
		return err
	}

	_ = ddl.DropTemporaryTable(ctx, s, temporaryTableName, false)
	return err
}
