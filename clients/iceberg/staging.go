package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/csvwriter"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/values"
)

func (s Store) EnsureNamespaceExists(ctx context.Context, namespace string) error {
	if err := s.apacheLivyClient.ExecContext(ctx, fmt.Sprintf("CREATE NAMESPACE IF NOT EXISTS `s3tablesbucket`.`%s`", namespace)); err != nil {
		return fmt.Errorf("failed to create namespace: %w", err)
	}

	return nil
}
func (s Store) AlterTable(ctx context.Context, tableID sql.TableIdentifier, cols []columns.Column) error {
	for _, col := range cols {
		colPart := fmt.Sprintf("%s %s", col.Name(), s.dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), config.SharedDestinationColumnSettings{}))
		query := s.dialect().BuildAddColumnQuery(tableID, colPart)
		if err := s.apacheLivyClient.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	return nil
}

func (s Store) CreateTable(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	var colParts []string
	for _, col := range cols {
		colPart := fmt.Sprintf("%s %s", col.Name(), s.dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), config.SharedDestinationColumnSettings{}))
		colParts = append(colParts, colPart)
	}

	if err := s.apacheLivyClient.ExecContext(ctx, s.dialect().BuildCreateTableQuery(tableID, false, colParts)); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Now add this to our [tableConfig]
	tableConfig.MutateInMemoryColumns(constants.Add, cols...)
	return nil
}

func (s Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tempTableID sql.TableIdentifier, createView bool) error {
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

	command := s.Dialect().BuildAppendCSVToTable(tempTableID, s3URI)
	if createView {
		command = s.Dialect().BuildCreateTemporaryView(tempTableID.EscapedTable(), s3URI)
	}

	if err := s.apacheLivyClient.ExecContext(ctx, command); err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	return nil
}

func (s *Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, error) {
	fp := filepath.Join(os.TempDir(), fmt.Sprintf("%s.csv", newTableID.FullyQualifiedName()))
	gzipWriter, err := csvwriter.NewGzipWriter(fp)
	if err != nil {
		return "", fmt.Errorf("failed to create gzip writer: %w", err)
	}

	defer gzipWriter.Close()

	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	headers := make([]string, len(columns))
	for _, col := range columns {
		headers = append(headers, col.Name())
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

func castColValStaging(colVal any, colKind typing.KindDetails) (string, error) {
	if colVal == nil {
		// TODO: What is the right way to express null?
		return `\\N`, nil
	}

	value, err := values.ToString(colVal, colKind)
	if err != nil {
		return "", err
	}

	return value, nil
}
