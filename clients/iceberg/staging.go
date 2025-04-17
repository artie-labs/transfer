package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/csvwriter"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"
)

func castColValStaging(colVal any, colKind typing.KindDetails) (string, error) {
	if colVal == nil {
		return constants.NullValuePlaceholder, nil
	}

	value, err := values.ToString(colVal, colKind)
	if err != nil {
		return "", err
	}

	return value, nil
}

func (s Store) uploadToS3(ctx context.Context, fp string) (string, error) {
	s3URI, err := awslib.UploadLocalFileToS3(ctx, awslib.UploadArgs{
		Bucket:                     s.config.Iceberg.S3Tables.Bucket,
		FilePath:                   fp,
		OverrideAWSAccessKeyID:     s.config.Iceberg.S3Tables.AwsAccessKeyID,
		OverrideAWSAccessKeySecret: s.config.Iceberg.S3Tables.AwsSecretAccessKey,
		Region:                     s.config.Iceberg.S3Tables.Region,
	})

	if err != nil {
		return "", fmt.Errorf("failed to upload to s3: %w", err)
	}

	// We need to change the prefix from s3:// to s3a://
	// Ref: https://stackoverflow.com/a/33356421
	s3URI = "s3a:" + strings.TrimPrefix(s3URI, "s3:")
	return s3URI, nil
}

func (s *Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, error) {
	fp := filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv.gz", strings.ReplaceAll(newTableID.FullyQualifiedName(), "`", "")))
	gzipWriter, err := csvwriter.NewGzipWriter(fp)
	if err != nil {
		return "", fmt.Errorf("failed to create gzip writer: %w", err)
	}

	gzipWriter.SetComma(',')

	defer gzipWriter.Close()

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	headers := make([]string, len(columns))
	for i, col := range columns {
		headers[i] = col.Name()
	}

	if err = gzipWriter.Write(headers); err != nil {
		return "", fmt.Errorf("failed to write headers: %w", err)
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

		if err = gzipWriter.Write(csvRow); err != nil {
			return "", fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	if err = gzipWriter.Flush(); err != nil {
		return "", fmt.Errorf("failed to flush gzip writer: %w", err)
	}

	return fp, nil
}

func (s Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tempTableID sql.TableIdentifier) error {
	fp, err := s.writeTemporaryTableFile(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		if deleteErr := os.RemoveAll(fp); deleteErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", deleteErr), slog.String("filePath", fp))
		}
	}()

	// Upload the file to S3
	s3URI, err := s.uploadToS3(ctx, fp)
	if err != nil {
		return fmt.Errorf("failed to upload to s3: %w", err)
	}

	// Create the temporary table
	if err = s.createTable(ctx, tempTableID, tableData.ReadOnlyInMemoryCols().ValidColumns()); err != nil {
		return fmt.Errorf("failed to create temporary table: %w", err)
	}

	// Load the data into a temporary view
	command := s.Dialect().BuildLoadCSV(tempTableID, s3URI)
	if err := s.apacheLivyClient.ExecContext(ctx, command); err != nil {
		return fmt.Errorf("failed to load temporary table: %w, command: %s", err, command)
	}

	return nil
}
