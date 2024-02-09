package snowflake

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/typing/values"

	"github.com/artie-labs/transfer/clients/utils"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/dml"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// castColValStaging - takes `colVal` interface{} and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func castColValStaging(colVal interface{}, colKind columns.Column, additionalDateFmts []string) (string, error) {
	if colVal == nil {
		// \\N needs to match NULL_IF(...) from ddl.go
		return `\\N`, nil
	}

	return values.ToString(colVal, colKind, additionalDateFmts)
}

// prepareTempTable does the following:
// 1) Create the temporary table
// 2) Load in-memory table -> CSV
// 3) Runs PUT to upload CSV to Snowflake staging (auto-compression with GZIP)
// 4) Runs COPY INTO with the columns specified into temporary table
// 5) Deletes CSV generated from (2)
func (s *Store) prepareTempTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string, additionalCopyClause string) error {
	if tableData.Mode() != config.History {
		tempAlterTableArgs := ddl.AlterTableArgs{
			Dwh:               s,
			Tc:                tableConfig,
			FqTableName:       tempTableName,
			CreateTable:       true,
			TemporaryTable:    true,
			ColumnOp:          constants.Add,
			UppercaseEscNames: &s.config.SharedDestinationConfig.UppercaseEscapedNames,
		}

		if err := ddl.AlterTable(tempAlterTableArgs, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	fp, err := s.loadTemporaryTable(tableData, tempTableName)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		// In the case where PUT or COPY fails, we'll at least delete the temporary file.
		if deleteErr := os.RemoveAll(fp); deleteErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", deleteErr), slog.String("filePath", fp))
		}
	}()

	if _, err = s.Exec(fmt.Sprintf("PUT file://%s @%s AUTO_COMPRESS=TRUE", fp, addPrefixToTableName(tempTableName, "%"))); err != nil {
		return fmt.Errorf("failed to run PUT for temporary table: %w", err)
	}

	copyCommand := fmt.Sprintf("COPY INTO %s (%s) FROM (SELECT %s FROM @%s)",
		// Copy into temporary tables (column ...)
		tempTableName, strings.Join(tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.config.SharedDestinationConfig.UppercaseEscapedNames, &sql.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}), ","),
		// Escaped columns, TABLE NAME
		escapeColumns(tableData.ReadOnlyInMemoryCols(), ","), addPrefixToTableName(tempTableName, "%"))

	if additionalCopyClause != "" {
		copyCommand += " " + additionalCopyClause
	}

	_, err = s.Exec(copyCommand)
	if err != nil {
		return fmt.Errorf("failed to load staging file into temporary table: %w", err)
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

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	for _, value := range tableData.Rows() {
		var row []string
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.config.SharedDestinationConfig.UppercaseEscapedNames, nil) {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal := value[col]
			// Check
			castedValue, castErr := castColValStaging(colVal, colKind, additionalDateFmts)
			if castErr != nil {
				return "", castErr
			}

			row = append(row, castedValue)
		}

		if err = writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	writer.Flush()
	return filePath, writer.Error()
}

func (s *Store) mergeWithStages(tableData *optimization.TableData) error {
	// TODO - better test coverage for `mergeWithStages`
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	fqName := tableData.ToFqName(s.Label(), true, s.config.SharedDestinationConfig.UppercaseEscapedNames, "")
	tableConfig, err := s.getTableConfig(fqName, tableData.TopicConfig.DropDeletedColumns)
	if err != nil {
		return err
	}

	// Check if all the columns exist in Snowflake
	srcKeysMissing, targetKeysMissing := columns.Diff(tableData.ReadOnlyInMemoryCols(), tableConfig.Columns(),
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
		UppercaseEscNames:      &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	err = ddl.AlterTable(deleteAlterTableArgs, srcKeysMissing...)
	if err != nil {
		slog.Warn("Failed to apply alter table", slog.Any("err", err))
		return err
	}

	tableConfig.AuditColumnsToDelete(srcKeysMissing)
	tableData.MergeColumnsFromDestination(tableConfig.Columns().GetColumns()...)
	temporaryTableName := fmt.Sprintf("%s_%s", tableData.ToFqName(s.Label(), false, s.config.SharedDestinationConfig.UppercaseEscapedNames, ""), tableData.TempTableSuffix())
	if err = s.prepareTempTable(tableData, tableConfig, temporaryTableName, ""); err != nil {
		return err
	}

	defer func() {
		if dropErr := ddl.DropTemporaryTable(s, temporaryTableName, false); dropErr != nil {
			slog.Warn("Failed to drop temporary table", slog.Any("err", dropErr), slog.String("tableName", temporaryTableName))
		}
	}()

	// Now iterate over all the in-memory cols and see which one requires backfill.
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.ShouldSkip() {
			continue
		}

		err = utils.BackfillColumn(s.config, s, col, fqName)
		if err != nil {
			return fmt.Errorf("failed to backfill col: %v, default value: %v, err: %w", col.RawName(), col.RawDefaultValue(), err)
		}

		tableConfig.Columns().UpsertColumn(col.RawName(), columns.UpsertColumnArg{
			Backfilled: ptr.ToBool(true),
		})
	}

	mergeArg := dml.MergeArgument{
		FqTableName:       fqName,
		SubQuery:          temporaryTableName,
		IdempotentKey:     tableData.TopicConfig.IdempotentKey,
		PrimaryKeys:       tableData.PrimaryKeys(s.config.SharedDestinationConfig.UppercaseEscapedNames, &sql.NameArgs{Escape: true, DestKind: s.Label()}),
		ColumnsToTypes:    *tableData.ReadOnlyInMemoryCols(),
		SoftDelete:        tableData.TopicConfig.SoftDelete,
		UppercaseEscNames: &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	mergeQuery, err := mergeArg.GetStatement()
	if err != nil {
		return fmt.Errorf("failed to generate merge statement: %w", err)
	}

	slog.Debug("Executing...", slog.String("query", mergeQuery))
	_, err = s.Exec(mergeQuery)
	return err
}
