package redshift

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tempTableID sql.TableIdentifier, parentTableID sql.TableIdentifier, opts types.AdditionalSettings, createTempTable bool) error {
	fp, colToNewLengthMap, err := s.loadTemporaryTable(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	for colName, newValue := range colToNewLengthMap {
		// Try to upsert columns first. If this fails, we won't need to update the destination table.
		if err = tableConfig.UpsertColumn(colName, columns.UpsertColumnArg{StringPrecision: typing.ToPtr(newValue)}); err != nil {
			return fmt.Errorf("failed to update table config with new string precision: %w", err)
		}

		if _, err = s.ExecContext(ctx, s.dialect().BuildIncreaseStringPrecisionQuery(parentTableID, colName, newValue)); err != nil {
			return fmt.Errorf("failed to increase string precision for table %q: %w", parentTableID.FullyQualifiedName(), err)
		}
	}

	if createTempTable {
		if err = shared.CreateTempTable(ctx, s, tableData, tableConfig, opts.ColumnSettings, tempTableID); err != nil {
			return err
		}
	}

	defer func() {
		// Remove file regardless of outcome to avoid fs build up.
		if removeErr := os.RemoveAll(fp); removeErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", removeErr), slog.String("filePath", fp))
		}
	}()

	s3Client, err := s.BuildS3Client(ctx)
	if err != nil {
		return fmt.Errorf("failed to build s3 client: %w", err)
	}

	s3Uri, err := s3Client.UploadLocalFileToS3(ctx, s.bucket, s.optionalS3Prefix, fp)
	if err != nil {
		return fmt.Errorf("failed to upload %q to s3: %w", fp, err)
	}

	var cols []string
	for _, col := range tableData.ReadOnlyInMemoryCols().ValidColumns() {
		cols = append(cols, col.Name())
	}

	credentialsClause, err := s.BuildCredentialsClause(ctx)
	if err != nil {
		return fmt.Errorf("failed to build credentials clause: %w", err)
	}

	copyStmt := s.dialect().BuildCopyStatement(tempTableID, cols, s3Uri, credentialsClause)
	if _, err = s.ExecContext(ctx, copyStmt); err != nil {
		return fmt.Errorf("failed to run COPY for temporary table: %w", err)
	}

	// Ref: https://docs.aws.amazon.com/redshift/latest/dg/PG_LAST_COPY_COUNT.html
	var rowsLoaded int64
	if err = s.QueryRowContext(ctx, `SELECT pg_last_copy_count();`).Scan(&rowsLoaded); err != nil {
		return fmt.Errorf("failed to check rows loaded: %w", err)
	}

	if rowsLoaded != int64(tableData.NumberOfRows()) {
		return fmt.Errorf("expected %d rows to be loaded, but got %d", tableData.NumberOfRows(), rowsLoaded)
	}

	return nil
}

func (s *Store) loadTemporaryTable(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, map[string]int32, error) {
	tempTableDataFile := shared.NewTemporaryDataFile(newTableID)
	file, additionalOutput, err := tempTableDataFile.WriteTemporaryTableFile(tableData, castColValStaging, s.config.SharedDestinationSettings)
	if err != nil {
		return "", nil, fmt.Errorf("failed to write temporary table file: %w", err)
	}

	// This will update the staging columns with the new string precision.
	for colName, newLength := range additionalOutput.ColumnToNewLengthMap {
		tableData.InMemoryColumns().UpsertColumn(colName, columns.UpsertColumnArg{
			StringPrecision: typing.ToPtr(newLength),
		})
	}

	return file.FilePath, additionalOutput.ColumnToNewLengthMap, nil
}
