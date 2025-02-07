package s3tables

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/values"
)

// func buildColumnSQLParts(dialect sql.Dialect, cols []columns.Column, settings config.SharedDestinationColumnSettings) []string {
// 	colSQLParts := make([]string, 0, len(cols))
// 	for _, col := range cols {
// 		colSQLParts = append(colSQLParts, dialect.DataTypeForKind(col.KindDetails, col.PrimaryKey(), settings))
// 	}

// 	return colSQLParts
// }

func (s Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, dwh *types.DestinationTableConfig, tempTableID sql.TableIdentifier, _ sql.TableIdentifier, additionalSettings types.AdditionalSettings, createTempTable bool) error {
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

	s3URI = "s3a:" + strings.TrimPrefix(s3URI, "s3:")
	// Step 2 - Load the CSV into a temporary view
	command := fmt.Sprintf(`
CREATE OR REPLACE TEMPORARY VIEW %s
USING csv
OPTIONS (
  path '%s',
  header 'true',
  compression 'gzip',
  nullValue '\\N',
  inferSchema 'true'
);
`, tempTableID.EscapedTable(), s3URI)

	fmt.Println("command", command, "fp", fp)
	if err := s.apacheLivyClient.ExecContext(ctx, command); err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
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
