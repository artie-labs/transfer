package snowflake

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/maputil"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
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

func castColValStaging(colVal any, colKind typing.KindDetails, config config.SharedDestinationSettings) (shared.ValueConvertResponse, error) {
	if colVal == nil {
		return shared.ValueConvertResponse{Value: constants.NullValuePlaceholder}, nil
	}

	value, err := values.ToString(colVal, colKind)
	if err != nil {
		// if config.SkipBadTimestamps {
		if parseError, ok := typing.BuildParseError(err); ok {
			if parseError.GetKind() == typing.UnsupportedDateLayout || parseError.GetKind() == typing.YearOutOfRange {
				slog.Info("Skipping a bad timestamp, returning null", slog.Any("err", err), slog.Any("value", colVal))
				return shared.ValueConvertResponse{Value: constants.NullValuePlaceholder}, nil
			}
		}
		// }

		return shared.ValueConvertResponse{}, err
	}

	return shared.ValueConvertResponse{Value: replaceExceededValues(value, colKind)}, nil
}

func (s Store) useExternalStage() bool {
	return s.config.Snowflake.ExternalStage != nil && s.config.Snowflake.ExternalStage.Enabled
}

func (s *Store) LoadDataIntoTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tempTableID, _ sql.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		if err := shared.CreateTempTable(ctx, s, tableData, dwh, additionalSettings.ColumnSettings, tempTableID); err != nil {
			return err
		}
	}

	if s.config.Snowflake.Streaming && s.snowpipeStreamingChannelManager != nil {
		castedTempTableID, ok := tempTableID.(dialect.TableIdentifier)
		if !ok {
			return fmt.Errorf("failed to cast temp table ID to TableIdentifier")
		}

		// Create the pipe if it doesn't exist
		pipeName := castedTempTableID.Table() + "_PIPE"
		pipe := dialect.NewTableIdentifier(castedTempTableID.Database(), castedTempTableID.Schema(), pipeName)
		columnNames := columns.ColumnNames(tableData.ReadOnlyInMemoryCols().ValidColumns())

		createPipeQuery := s.dialect().BuildCreatePipeQuery(
			pipe,
			castedTempTableID,
			columnNames,
		)

		if _, err := s.ExecContext(ctx, createPipeQuery); err != nil {
			return fmt.Errorf("failed to create pipe for streaming: %w", err)
		}

		return s.snowpipeStreamingChannelManager.LoadData(ctx, castedTempTableID.Database(), castedTempTableID.Schema(), pipeName, time.Now(), *tableData)
	}

	// Write data into CSV
	tempTableDataFile := shared.NewTemporaryDataFile(tempTableID)
	file, _, err := tempTableDataFile.WriteTemporaryTableFile(tableData, castColValStaging, s.config.SharedDestinationSettings)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		// In the case where PUT or COPY fails, we'll at least delete the temporary file.
		if deleteErr := os.RemoveAll(file.FilePath); deleteErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", deleteErr), slog.String("filePath", file.FilePath))
		}
	}()

	tableStageName := addPrefixToTableName(tempTableID, "%")
	if s.useExternalStage() {
		s3Client, err := s.GetS3Client()
		if err != nil {
			return fmt.Errorf("failed to get S3 client: %w", err)
		}

		_, err = s3Client.UploadLocalFileToS3(
			ctx,
			s.config.Snowflake.ExternalStage.Bucket,
			s.config.Snowflake.ExternalStage.Prefix,
			file.FilePath,
		)
		if err != nil {
			return fmt.Errorf("failed to upload file to S3: %w", err)
		}

		castedTableID, ok := tempTableID.(dialect.TableIdentifier)
		if !ok {
			return fmt.Errorf("failed to cast table identifier: %w", err)
		}

		tableStageName = fmt.Sprintf("%s.%s.%s/", castedTableID.Database(), castedTableID.Schema(), s.config.Snowflake.ExternalStage.Name)
	} else {
		// Upload the CSV file to Snowflake internal stage
		putQuery := fmt.Sprintf("PUT 'file://%s' @%s", file.FilePath, tableStageName)
		if _, err = s.ExecContext(ctx, putQuery); err != nil {
			return fmt.Errorf("failed to run PUT for temporary table: %w", err)
		}
	}

	copyCommand := s.dialect().BuildCopyIntoTableQuery(tempTableID, tableData.ReadOnlyInMemoryCols().ValidColumns(), tableStageName, file.FileName)
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
