package snowflake

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/values"
)

func replaceExceededValues(colVal string, colKind columns.Column) string {
	// https://community.snowflake.com/s/article/Max-LOB-size-exceeded
	const maxLobLength = 16777216
	if colKind.KindDetails.Kind == typing.Struct.Kind {
		if len(colVal) > maxLobLength {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker)
		}
	}

	return colVal
}

// castColValStaging - takes `colVal` any and `colKind` typing.Column and converts the value into a string value
// This is necessary because CSV writers require values to in `string`.
func castColValStaging(colVal any, colKind columns.Column, additionalDateFmts []string) (string, error) {
	if colVal == nil {
		// \\N needs to match NULL_IF(...) from ddl.go
		return `\\N`, nil
	}

	value, err := values.ToString(colVal, colKind, additionalDateFmts)
	if err != nil {
		return "", err
	}

	return replaceExceededValues(value, colKind), nil
}

func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID types.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	tempTableName := tempTableID.FullyQualifiedName()

	if createTempTable {
		tempAlterTableArgs := ddl.AlterTableArgs{
			Dwh:               s,
			Tc:                tableConfig,
			TableID:           tempTableID,
			CreateTable:       true,
			TemporaryTable:    true,
			ColumnOp:          constants.Add,
			UppercaseEscNames: ptr.ToBool(s.ShouldUppercaseEscapedNames()),
			Mode:              tableData.Mode(),
		}

		if err := tempAlterTableArgs.AlterTable(tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	// Write data into CSV
	fp, err := s.writeTemporaryTableFile(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		// In the case where PUT or COPY fails, we'll at least delete the temporary file.
		if deleteErr := os.RemoveAll(fp); deleteErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", deleteErr), slog.String("filePath", fp))
		}
	}()

	// Upload the CSV file to Snowflake
	if _, err = s.Exec(fmt.Sprintf("PUT file://%s @%s AUTO_COMPRESS=TRUE", fp, addPrefixToTableName(tempTableID, "%"))); err != nil {
		return fmt.Errorf("failed to run PUT for temporary table: %w", err)
	}

	// COPY the CSV file (in Snowflake) into a table
	copyCommand := fmt.Sprintf("COPY INTO %s (%s) FROM (SELECT %s FROM @%s)",
		tempTableName, strings.Join(tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.ShouldUppercaseEscapedNames(), &sql.NameArgs{
			Escape:   true,
			DestKind: s.Label(),
		}), ","),
		escapeColumns(tableData.ReadOnlyInMemoryCols(), ","), addPrefixToTableName(tempTableID, "%"))

	if additionalSettings.AdditionalCopyClause != "" {
		copyCommand += " " + additionalSettings.AdditionalCopyClause
	}

	if _, err = s.Exec(copyCommand); err != nil {
		return fmt.Errorf("failed to run copy into temporary table: %w", err)
	}

	return nil
}

func (s *Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID types.TableIdentifier) (string, error) {
	fp := filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv", newTableID.FullyQualifiedName()))
	file, err := os.Create(fp)
	if err != nil {
		return "", err
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = '\t'

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	for _, value := range tableData.Rows() {
		var row []string
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.ShouldUppercaseEscapedNames(), nil) {
			column, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			castedValue, castErr := castColValStaging(value[col], column, additionalDateFmts)
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
	return fp, writer.Error()
}
