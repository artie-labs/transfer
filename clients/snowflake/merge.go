package snowflake

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/artie-labs/transfer/lib/logger"

	"github.com/artie-labs/transfer/lib/dwh/types"

	"github.com/artie-labs/transfer/lib/dwh/ddl"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

// escapeCols will return the following arguments:
// 1) colsToUpdate - list of columns to update
// 2) list of columns to update (escaped).
func escapeCols(cols []typing.Column) (colsToUpdate []string, colsToUpdateEscaped []string) {
	for _, column := range cols {
		if column.KindDetails.Kind == typing.Invalid.Kind {
			// Don't update Snowflake
			continue
		}

		escapedCol := column.Name
		switch column.KindDetails.Kind {
		case typing.Struct.Kind, typing.Array.Kind:
			if column.ToastColumn {
				escapedCol = fmt.Sprintf("CASE WHEN %s = '%s' THEN {'key': '%s'} ELSE PARSE_JSON(%s) END %s",
					// Comparing the column against placeholder
					column.Name, constants.ToastUnavailableValuePlaceholder,
					// Casting placeholder as a JSON object
					constants.ToastUnavailableValuePlaceholder,
					// Regular parsing.
					column.Name, column.Name)
			} else {
				escapedCol = fmt.Sprintf("PARSE_JSON(%s) %s", column.Name, column.Name)
			}
		}

		colsToUpdate = append(colsToUpdate, column.Name)
		colsToUpdateEscaped = append(colsToUpdateEscaped, escapedCol)
	}

	return
}

// PrepareTemporaryTable does the following:
// 1) Create the temporary table
// 2) Load in-memory table -> CSV
// 3) Runs PUT to upload CSV to Snowflake staging (auto-compression with GZIP)
// 4) Runs COPY INTO with the columns specified into temporary table
// 5) Deletes CSV generated from (2)
func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string) error {
	// TODO - how do I delete staging table?

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

	fp, err := s.loadTemporaryTable(tableData, tempTableName)
	if err != nil {
		return fmt.Errorf("failed to load temporary table, err: %v", err)
	}

	if _, err = s.Exec(fmt.Sprintf("PUT file://%s @%s AUTO_COMPRESS=TRUE", fp, tempTableName)); err != nil {
		return fmt.Errorf("failed to run PUT for temporary table, err: %v", err)
	}

	if _, err = s.Exec(fmt.Sprintf("COPY INTO %s (%s)", tempTableName, strings.Join(tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(), ","))); err != nil {
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
func (s *Store) loadTemporaryTable(tableData *optimization.TableData, newTableName string) (string, error) {
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
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate() {
			row = append(row, fmt.Sprint(value[col]))
		}

		if err = writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write to csv, err: %v", err)
		}
	}

	writer.Flush()
	return filePath, writer.Error()
}

func (s *Store) deleteTemporaryFile(tableName string) error {
	// TODO - test
	return os.RemoveAll(fmt.Sprintf("/tmp/%s.csv", tableName))
}
