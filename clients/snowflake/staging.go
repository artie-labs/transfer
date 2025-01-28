package snowflake

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"
)

func replaceExceededValues(colVal string, kindDetails typing.KindDetails) string {
	// https://community.snowflake.com/s/article/Max-LOB-size-exceeded
	const maxLobLength int32 = 16777216
	switch kindDetails.Kind {
	case typing.Struct.Kind:
		if int32(len(colVal)) > maxLobLength {
			return fmt.Sprintf(`{"key":"%s"}`, constants.ExceededValueMarker)
		}
	case typing.String.Kind:
		maxLength := typing.DefaultValueFromPtr[int32](kindDetails.OptionalStringPrecision, maxLobLength)
		if int32(len(colVal)) > maxLength {
			return constants.ExceededValueMarker
		}
	}

	return colVal
}

func castColValStaging(colVal any, colKind typing.KindDetails) (string, error) {
	if colVal == nil {
		// \\N needs to match NULL_IF(...) from ddl.go
		return `\\N`, nil
	}

	value, err := values.ToString(colVal, colKind)
	if err != nil {
		return "", err
	}

	return replaceExceededValues(value, colKind), nil
}

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DwhTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, additionalSettings.ColumnSettings, tempTableID); err != nil {
			return err
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
		tempTableID.FullyQualifiedName(),
		strings.Join(sql.QuoteColumns(tableData.ReadOnlyInMemoryCols().ValidColumns(), s.Dialect()), ","),
		escapeColumns(tableData.ReadOnlyInMemoryCols(), ","), addPrefixToTableName(tempTableID, "%"))

	if additionalSettings.AdditionalCopyClause != "" {
		copyCommand += " " + additionalSettings.AdditionalCopyClause
	}

	if _, err = s.Exec(copyCommand); err != nil {
		// For non-temp tables, we should try to delete the staging file if COPY INTO fails.
		// This is because [PURGE = TRUE] will only delete the staging files upon a successful COPY INTO.
		// We also only need to do this for non-temp tables because these staging files will linger, since we create a new temporary table per attempt.
		if !createTempTable {
			if _, deleteErr := s.ExecContext(ctx, s.dialect().BuildRemoveFilesFromStage(addPrefixToTableName(tempTableID, "%"), "")); deleteErr != nil {
				slog.Warn("Failed to remove all files from stage", slog.Any("deleteErr", deleteErr))
			}
		}

		return fmt.Errorf("failed to run copy into temporary table: %w", err)
	}

	return nil
}

func (s *Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, error) {
	fp := filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv", newTableID.FullyQualifiedName()))
	file, err := os.Create(fp)
	if err != nil {
		return "", err
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = '\t'

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()

	for _, col := range columns {
		fmt.Println("col", col.Name())
	}

	for _, row := range tableData.Rows() {
		var csvRow []string
		for _, col := range columns {
			castedValue, castErr := castColValStaging(row[col.Name()], col.KindDetails)
			if castErr != nil {
				return "", fmt.Errorf("failed to cast value '%v': %w", row[col.Name()], castErr)
			}

			csvRow = append(csvRow, castedValue)
		}

		if err = writer.Write(csvRow); err != nil {
			return "", fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	writer.Flush()
	return fp, writer.Error()
}
