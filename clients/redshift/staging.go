package redshift

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/s3lib"
	"github.com/artie-labs/transfer/lib/sql"
)

func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID sql.TableIdentifier, _ types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		tempAlterTableArgs := ddl.AlterTableArgs{
			Dialect:        s.Dialect(),
			Tc:             tableConfig,
			TableID:        tempTableID,
			CreateTable:    true,
			TemporaryTable: true,
			ColumnOp:       constants.Add,
			Mode:           tableData.Mode(),
		}

		if err := tempAlterTableArgs.AlterTable(s, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	fp, err := s.loadTemporaryTable(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	defer func() {
		// Remove file regardless of outcome to avoid fs build up.
		if removeErr := os.RemoveAll(fp); removeErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", removeErr), slog.String("filePath", fp))
		}
	}()

	// Load fp into s3, get S3 URI and pass it down.
	s3Uri, err := s3lib.UploadLocalFileToS3(context.Background(), s3lib.UploadArgs{
		OptionalS3Prefix: s.optionalS3Prefix,
		Bucket:           s.bucket,
		FilePath:         fp,
	})

	if err != nil {
		return fmt.Errorf("failed to upload %s to s3: %w", fp, err)
	}

	// COPY table_name FROM '/path/to/local/file' DELIMITER '\t' NULL '\\N' FORMAT csv;
	// Note, we need to specify `\\N` here and in `CastColVal(..)` we are only doing `\N`, this is because Redshift treats backslashes as an escape character.
	// So, it'll convert `\N` => `\\N` during COPY.
	copyStmt := fmt.Sprintf(
		`COPY %s FROM '%s' DELIMITER '\t' NULL AS '\\N' GZIP FORMAT CSV %s dateformat 'auto' timeformat 'auto';`,
		tempTableID.FullyQualifiedName(),
		s3Uri,
		s.credentialsClause,
	)
	if _, err = s.Exec(copyStmt); err != nil {
		return fmt.Errorf("failed to run COPY for temporary table: %w", err)
	}

	return nil
}

func (s *Store) loadTemporaryTable(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, error) {
	filePath := fmt.Sprintf("/tmp/%s.csv.gz", newTableID.FullyQualifiedName())
	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}

	defer file.Close()

	gzipWriter := gzip.NewWriter(file) // Create a new gzip writer
	defer gzipWriter.Close()           // Ensure to close the gzip writer after writing

	writer := csv.NewWriter(gzipWriter) // Create a CSV writer on top of the gzip writer
	writer.Comma = '\t'

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	for _, value := range tableData.Rows() {
		var row []string
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate() {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			castedValue, castErr := s.CastColValStaging(value[col], colKind, additionalDateFmts)
			fmt.Println("castedValue", castedValue, "colKind", colKind.KindDetails, "colName", colKind.Name())
			if castErr != nil {
				return "", castErr
			}

			row = append(row, castedValue)
		}

		if err = writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	writer.Flush()
	if err = writer.Error(); err != nil {
		return "", fmt.Errorf("failed to flush csv writer: %w", err)
	}

	return filePath, nil
}
