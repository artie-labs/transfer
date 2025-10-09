package redshift

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/artie-labs/transfer/clients/redshift/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config/constants"
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

func (s *Store) PrepareReusableStagingTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DestinationTableConfig, tempTableID sql.TableIdentifier, parentTableID sql.TableIdentifier) error {
	exists, err := s.CheckStagingTableExists(ctx, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to check if staging table exists: %w", err)
	}

	if exists {
		compatible, err := s.ValidateStagingTableSchema(ctx, tempTableID, tableData.ReadOnlyInMemoryCols().ValidColumns())
		if err != nil {
			return fmt.Errorf("failed to validate staging table schema: %w", err)
		}

		// Don't handle leftover rows in staging table, always truncate or drop before merging
		if !compatible {
			err := s.DropTable(ctx, tempTableID)
			if err != nil {
				return fmt.Errorf("failed to drop staging table: %w", err)
			}
			return s.PrepareReusableStagingTable(ctx, tableData, tableConfig, tempTableID, parentTableID)
		} else {
			if err := s.TruncateStagingTable(ctx, tempTableID); err != nil {
				return fmt.Errorf("failed to truncate staging table: %w", err)
			}
		}
	} else {
		if err := shared.CreateTempTable(ctx, s, tableData, tableConfig, types.AdditionalSettings{}.ColumnSettings, tempTableID); err != nil {
			return fmt.Errorf("failed to create staging table: %w", err)
		}
	}

	return s.loadDataIntoStagingTable(ctx, tableData, tempTableID, parentTableID)
}

func (s *Store) loadDataIntoStagingTable(ctx context.Context, tableData *optimization.TableData, tempTableID sql.TableIdentifier, parentTableID sql.TableIdentifier) error {
	fp, colToNewLengthMap, err := s.loadTemporaryTable(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	for colName, newValue := range colToNewLengthMap {
		// Try to upsert columns first. If this fails, we won't need to update the destination table.
		if err = tableData.InMemoryColumns().UpsertColumn(colName, columns.UpsertColumnArg{StringPrecision: typing.ToPtr(newValue)}); err != nil {
			return fmt.Errorf("failed to update table config with new string precision: %w", err)
		}

		if _, err = s.ExecContext(ctx, s.dialect().BuildIncreaseStringPrecisionQuery(parentTableID, colName, newValue)); err != nil {
			return fmt.Errorf("failed to increase string precision for table %q: %w", parentTableID.FullyQualifiedName(), err)
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

func (s *Store) CheckStagingTableExists(ctx context.Context, tableID sql.TableIdentifier) (bool, error) {
	redshiftTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return false, fmt.Errorf("failed to cast table identifier to Redshift TableIdentifier")
	}

	query := `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = $1 AND table_name = $2
	)`

	var exists bool
	err := s.QueryRowContext(ctx, query, redshiftTableID.Schema(), redshiftTableID.Table()).Scan(&exists)
	return exists, err
}

func (s *Store) ValidateStagingTableSchema(ctx context.Context, tableID sql.TableIdentifier, expectedColumns []columns.Column) (bool, error) {
	redshiftTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return false, fmt.Errorf("failed to cast table identifier to Redshift TableIdentifier")
	}

	query := `SELECT column_name, data_type FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`

	rows, err := s.QueryContext(ctx, query, redshiftTableID.Schema(), redshiftTableID.Table())
	if err != nil {
		return false, fmt.Errorf("failed to query table schema: %w", err)
	}
	defer rows.Close()

	currentColumns := make(map[string]string)
	for rows.Next() {
		var colName, dataType string
		if err := rows.Scan(&colName, &dataType); err != nil {
			return false, fmt.Errorf("failed to scan column info: %w", err)
		}
		currentColumns[colName] = dataType
	}

	for _, expectedCol := range expectedColumns {
		if _, exists := currentColumns[expectedCol.Name()]; !exists {
			return false, nil
		}
	}

	return true, nil
}

func (s *Store) TruncateStagingTable(ctx context.Context, tableID sql.TableIdentifier) error {
	if strings.Contains(strings.ToLower(tableID.Table()), constants.ArtiePrefix) {
		sqlCommand := s.Dialect().BuildTruncateTableQuery(tableID)
		if _, err := s.ExecContext(ctx, sqlCommand); err != nil {
			return fmt.Errorf("failed to truncate staging table: %w", err)
		}
	}
	return nil
}
