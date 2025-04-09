package snowflake

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/csvwriter"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/maputil"
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
		return constants.NullValuePlaceholder, nil
	}

	value, err := values.ToString(colVal, colKind)
	if err != nil {
		return "", err
	}

	return replaceExceededValues(value, colKind), nil
}

func (s Store) useExternalStage() bool {
	return s.config.Snowflake.ExternalStage != nil && s.config.Snowflake.ExternalStage.Enabled
}

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, additionalSettings.ColumnSettings, tempTableID); err != nil {
			return err
		}
	}

	// Write data into CSV
	file, err := s.writeTemporaryTableFile(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		// In the case where PUT or COPY fails, we'll at least delete the temporary file.
		if deleteErr := os.RemoveAll(file.FilePath); deleteErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", deleteErr), slog.String("filePath", file.FilePath))
		}
	}()

	if s.useExternalStage() {
		// Upload to S3 using our built-in library
		_, err = awslib.UploadLocalFileToS3(ctx, awslib.UploadArgs{
			Bucket:           s.config.Snowflake.ExternalStage.Bucket,
			OptionalS3Prefix: filepath.Join(s.config.Snowflake.ExternalStage.Prefix, tempTableID.FullyQualifiedName()),
			FilePath:         file.FilePath,
			Region:           os.Getenv("AWS_REGION"),
		})
		if err != nil {
			return fmt.Errorf("failed to upload file to S3: %w", err)
		}
	} else {
		// Upload the CSV file to Snowflake internal stage
		tableStageName := addPrefixToTableName(tempTableID, "%")
		putQuery := fmt.Sprintf("PUT 'file://%s' @%s AUTO_COMPRESS=TRUE", file.FilePath, tableStageName)
		if _, err = s.ExecContext(ctx, putQuery); err != nil {
			return fmt.Errorf("failed to run PUT for temporary table: %w", err)
		}
	}

	tableStageName := addPrefixToTableName(tempTableID, "%")
	// We are appending gz to the file name since it was compressed by the PUT command.
	fileName := fmt.Sprintf("%s.gz", file.FileName)
	if s.useExternalStage() {
		tableStageName = filepath.Join(s.config.Snowflake.ExternalStage.ExternalStageName, s.config.Snowflake.ExternalStage.Prefix, tempTableID.FullyQualifiedName())
		// We don't need to append .gz to the file name since it was already compressed by [s.writeTemporaryTableFileGZIP]
		fileName = file.FileName
	}

	copyCommand := s.dialect().BuildCopyIntoTableQuery(tempTableID, tableData.ReadOnlyInMemoryCols().ValidColumns(), tableStageName, fileName)
	if additionalSettings.AdditionalCopyClause != "" {
		copyCommand += " " + additionalSettings.AdditionalCopyClause
	}

	// COPY INTO does not implement [RowsAffected]. Instead, we'll treat this as a query and then parse the output:
	// https://docs.snowflake.com/en/sql-reference/sql/copy-into-table#output
	sqlRows, err := s.QueryContext(ctx, copyCommand)
	if err != nil {
		// For non-temp tables, we should try to delete the staging file if COPY INTO fails.
		// This is because [PURGE = TRUE] will only delete the staging files upon a successful COPY INTO.
		// We also only need to do this for non-temp tables because these staging files will linger, since we create a new temporary table per attempt.
		if !createTempTable && !s.useExternalStage() {
			if _, deleteErr := s.ExecContext(ctx, s.dialect().BuildRemoveFilesFromStage(addPrefixToTableName(tempTableID, "%"), "")); deleteErr != nil {
				slog.Warn("Failed to remove all files from stage", slog.Any("deleteErr", deleteErr))
			}
		}

		return fmt.Errorf("failed to run copy into temporary table: %w", err)
	}

	rows, err := sql.RowsToObjects(sqlRows)
	if err != nil {
		return fmt.Errorf("failed to convert rows to objects: %w", err)
	}

	var rowsLoaded int64
	for _, row := range rows {
		rowsLoadedStr, err := maputil.GetTypeFromMap[string](row, "rows_loaded")
		if err != nil {
			return fmt.Errorf("failed to get rows loaded: %w", err)
		}

		_rowsLoaded, err := strconv.ParseInt(rowsLoadedStr, 10, 64)
		if err != nil {
			return fmt.Errorf("failed to parse rows loaded: %w", err)
		}

		rowsLoaded += _rowsLoaded
	}

	expectedRows := int64(len(tableData.Rows()))
	if rowsLoaded != expectedRows {
		return fmt.Errorf("expected %d rows to be inserted, but got %d", expectedRows, rowsLoaded)
	}

	return nil
}

type File struct {
	FilePath string
	FileName string
}

func (s *Store) writeTemporaryTableFileGZIP(tableData *optimization.TableData, newTableID sql.TableIdentifier) (File, error) {
	fp := filepath.Join(os.TempDir(), strings.ReplaceAll(newTableID.FullyQualifiedName(), `"`, ""))
	gzipWriter, err := csvwriter.NewGzipWriter(fp)
	if err != nil {
		return File{}, fmt.Errorf("failed to create gzip writer: %w", err)
	}

	defer gzipWriter.Close()

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	for _, value := range tableData.Rows() {
		var row []string
		for _, col := range columns {
			castedValue, castErr := castColValStaging(value[col.Name()], col.KindDetails)
			if castErr != nil {
				return File{}, castErr
			}

			row = append(row, castedValue)
		}

		if err = gzipWriter.Write(row); err != nil {
			return File{}, fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	if err = gzipWriter.Flush(); err != nil {
		return File{}, fmt.Errorf("failed to flush gzip writer: %w", err)
	}

	return File{FilePath: fp, FileName: gzipWriter.FileName()}, nil
}

// TODO: Deprecate this in favor of writing GZIP delta files directly without relying on Snowflake's auto compression
func (s *Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier) (File, error) {
	if s.useExternalStage() {
		return s.writeTemporaryTableFileGZIP(tableData, newTableID)
	}

	fileName := fmt.Sprintf("%s.csv", strings.ReplaceAll(newTableID.FullyQualifiedName(), `"`, ""))
	fp := filepath.Join(os.TempDir(), fileName)
	file, err := os.Create(fp)
	if err != nil {
		return File{}, err
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = '\t'

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	for _, row := range tableData.Rows() {
		var csvRow []string
		for _, col := range columns {
			castedValue, castErr := castColValStaging(row[col.Name()], col.KindDetails)
			if castErr != nil {
				return File{}, fmt.Errorf("failed to cast value '%v': %w", row[col.Name()], castErr)
			}

			csvRow = append(csvRow, castedValue)
		}

		if err = writer.Write(csvRow); err != nil {
			return File{}, fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	writer.Flush()
	return File{FilePath: fp, FileName: fileName}, writer.Error()
}
