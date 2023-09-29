package redshift

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"time"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/s3lib"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *Store) prepareTempTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string) error {
	tempAlterTableArgs := ddl.AlterTableArgs{
		Dwh:            s,
		Tc:             tableConfig,
		FqTableName:    tempTableName,
		CreateTable:    true,
		TemporaryTable: true,
		ColumnOp:       constants.Add,
	}

	if err := ddl.AlterTable(ctx, tempAlterTableArgs, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
		return fmt.Errorf("failed to create temp table, error: %v", err)
	}

	expiryString := typing.ExpiresDate(time.Now().UTC().Add(ddl.TempTableTTL))
	// Now add a comment to the temporary table.
	if _, err := s.Exec(fmt.Sprintf(`COMMENT ON TABLE %s IS '%s';`, tempTableName, ddl.ExpiryComment(expiryString))); err != nil {
		return fmt.Errorf("failed to add comment to table, tableName: %v, err: %v", tempTableName, err)
	}

	fp, err := s.loadTemporaryTable(ctx, tableData, tempTableName)
	if err != nil {
		return fmt.Errorf("failed to load temporary table, err: %v", err)
	}

	// Load fp into s3, get S3 URI and pass it down.
	s3Uri, err := s3lib.UploadLocalFileToS3(ctx, s3lib.UploadArgs{
		OptionalS3Prefix: s.optionalS3Prefix,
		Bucket:           s.bucket,
		FilePath:         fp,
	})

	if err != nil {
		return fmt.Errorf("failed to upload this to s3, err: %v", err)
	}

	// COPY table_name FROM '/path/to/local/file' DELIMITER '\t' NULL '\\N' FORMAT csv;
	// Note, we need to specify `\\N` here and in `CastColVal(..)` we are only doing `\N`, this is because Redshift treats backslashes as an escape character.
	// So, it'll convert `\N` => `\\N` during COPY.
	copyStmt := fmt.Sprintf(`COPY %s FROM '%s' DELIMITER '\t' NULL AS '\\N' GZIP FORMAT CSV %s dateformat 'auto' timeformat 'auto';`, tempTableName, s3Uri, s.credentialsClause)
	if _, err = s.Exec(copyStmt); err != nil {
		return fmt.Errorf("failed to run COPY for temporary table, err: %v, copy: %v", err, copyStmt)
	}

	if deleteErr := os.RemoveAll(fp); deleteErr != nil {
		logger.FromContext(ctx).WithError(deleteErr).WithField("filePath", fp).Warn("failed to delete temp file")
	}

	return nil
}

func (s *Store) loadTemporaryTable(ctx context.Context, tableData *optimization.TableData, newTableName string) (string, error) {
	filePath := fmt.Sprintf("/tmp/%s.csv.gz", newTableName)
	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file) // Create a new gzip writer
	defer gzipWriter.Close()           // Ensure to close the gzip writer after writing

	writer := csv.NewWriter(gzipWriter) // Create a CSV writer on top of the gzip writer
	writer.Comma = '\t'
	for _, value := range tableData.RowsData() {
		var row []string
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(ctx, nil) {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			castedValue, castErr := s.CastColValStaging(ctx, value[col], colKind)
			if castErr != nil {
				return "", castErr
			}
			row = append(row, castedValue)
		}

		if err = writer.Write(row); err != nil {
			return "", fmt.Errorf("failed to write to csv, err: %v", err)
		}
	}

	writer.Flush()
	if err = writer.Error(); err != nil {
		return "", fmt.Errorf("failed to flush csv writer, err: %v", err)
	}

	return filePath, nil
}
