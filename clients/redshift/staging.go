package redshift

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/csvwriter"
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

	args := awslib.UploadArgs{
		OptionalS3Prefix: s.optionalS3Prefix,
		Bucket:           s.bucket,
		FilePath:         fp,
		Region:           os.Getenv("AWS_REGION"),
	}

	if s._awsCredentials != nil {
		creds, err := s._awsCredentials.BuildCredentials(ctx)
		if err != nil {
			return fmt.Errorf("failed to build credentials: %w", err)
		}

		args.OverrideAWSAccessKeyID = creds.Value.AccessKeyID
		args.OverrideAWSAccessKeySecret = creds.Value.SecretAccessKey
		args.OverrideAWSSessionToken = creds.Value.SessionToken
	}

	// Load fp into s3, get S3 URI and pass it down.
	s3Uri, err := awslib.UploadLocalFileToS3(ctx, args)
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
	filePath := fmt.Sprintf("/tmp/%s.csv.gz", newTableID.FullyQualifiedName())
	gzipWriter, err := csvwriter.NewGzipWriter(filePath)
	if err != nil {
		return "", nil, err
	}

	defer gzipWriter.Close()

	_columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	columnToNewLengthMap := make(map[string]int32)
	for _, value := range tableData.Rows() {
		var row []string
		for _, col := range _columns {
			result, err := castColValStaging(
				value[col.Name()],
				col.KindDetails,
				s.config.SharedDestinationSettings.TruncateExceededValues,
				s.config.SharedDestinationSettings.ExpandStringPrecision,
			)

			if err != nil {
				return "", nil, err
			}

			if result.NewLength > 0 {
				_newLength, isOk := columnToNewLengthMap[col.Name()]
				if !isOk || result.NewLength > _newLength {
					// Update the new length if it's greater than the current one.
					columnToNewLengthMap[col.Name()] = result.NewLength
				}
			}
			row = append(row, result.Value)
		}

		if err = gzipWriter.Write(row); err != nil {
			return "", nil, fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	if err = gzipWriter.Flush(); err != nil {
		return "", nil, fmt.Errorf("failed to flush and close the gzip writer: %w", err)
	}

	// This will update the staging columns with the new string precision.
	for colName, newLength := range columnToNewLengthMap {
		tableData.InMemoryColumns().UpsertColumn(colName, columns.UpsertColumnArg{
			StringPrecision: typing.ToPtr(newLength),
		})
	}

	return filePath, columnToNewLengthMap, nil
}
