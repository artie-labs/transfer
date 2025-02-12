package iceberg

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"
)

func (s Store) uploadToS3(ctx context.Context, fp string) (string, error) {
	s3URI, err := awslib.UploadLocalFileToS3(ctx, awslib.UploadArgs{
		Bucket:                     s.config.Iceberg.S3Tables.Bucket,
		FilePath:                   fp,
		OverrideAWSAccessKeyID:     &s.config.Iceberg.S3Tables.AwsAccessKeyID,
		OverrideAWSAccessKeySecret: &s.config.Iceberg.S3Tables.AwsSecretAccessKey,
	})

	if err != nil {
		return "", fmt.Errorf("failed to upload to s3: %w", err)
	}

	// We need to change the prefix from s3:// to s3a://
	// Ref: https://stackoverflow.com/a/33356421
	s3URI = "s3a:" + strings.TrimPrefix(s3URI, "s3:")
	return s3URI, nil
}

func castColValStaging(colVal any, colKind typing.KindDetails) (string, error) {
	if colVal == nil {
		return constants.NullValuePlaceholder, nil
	}

	return values.ToString(colVal, colKind)
}

func (s *Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, error) {
	fp := filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv.gz", newTableID.FullyQualifiedName()))
	file, err := os.Create(fp)
	if err != nil {
		return "", err
	}

	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()
	writer := csv.NewWriter(gzipWriter)
	writer.Comma = '\t'

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()

	// Write out the headers.
	headers := make([]string, len(columns))
	for _, col := range columns {
		headers = append(headers, col.Name())
	}

	if err = writer.Write(headers); err != nil {
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

		if err = writer.Write(csvRow); err != nil {
			return "", fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	writer.Flush()
	return fp, writer.Error()
}

func (s Store) prepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, tempTableID sql.TableIdentifier, createView bool) error {
	fp, err := s.writeTemporaryTableFile(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		if deleteErr := os.RemoveAll(fp); deleteErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", deleteErr), slog.String("filePath", fp))
		}
	}()

	// Step 1 - Upload the file to S3
	s3URI, err := s.uploadToS3(ctx, fp)
	if err != nil {
		return fmt.Errorf("failed to upload to s3: %w", err)
	}

	// Step 2 - Load the CSV into a temporary view, or directly into a table depending on [createView]
	command := s.Dialect().BuildAppendCSVToTable(tempTableID, s3URI)
	if createView {
		command = s.Dialect().BuildCreateTemporaryView(tempTableID.EscapedTable(), s3URI)
	}

	fmt.Println("command", command)

	// Step 3 - Submit the command to Spark via Apache Livy
	if err := s.apacheLivyClient.ExecContext(ctx, command); err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	return nil
}
