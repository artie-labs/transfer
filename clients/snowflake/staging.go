package snowflake

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/values"
)

// castColValStaging - takes `colVal` any and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func castColValStaging(colVal any, colKind columns.Column, additionalDateFmts []string) (string, error) {
	if colVal == nil {
		// \\N needs to match NULL_IF(...) from ddl.go
		return `\\N`, nil
	}

	return values.ToString(colVal, colKind, additionalDateFmts)
}

// PrepareTemporaryTable does the following:
// 1) Create the temporary table
// 2) Load in-memory table -> CSV
// 3) Runs PUT to upload CSV to Snowflake staging (auto-compression with GZIP)
// 4) Runs COPY INTO with the columns specified into temporary table
// 5) Deletes CSV generated from (2)
func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		tempAlterTableArgs := ddl.AlterTableArgs{
			Dwh:               s,
			Tc:                tableConfig,
			FqTableName:       tempTableName,
			CreateTable:       true,
			TemporaryTable:    true,
			ColumnOp:          constants.Add,
			UppercaseEscNames: &s.config.SharedDestinationConfig.UppercaseEscapedNames,
			Mode:              tableData.Mode(),
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

	if additionalSettings.AdditionalCopyClause != "" {
		copyCommand += " " + additionalSettings.AdditionalCopyClause
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
