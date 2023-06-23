package redshift

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/artie-labs/transfer/lib/s3"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/dwh/ddl"
	"github.com/artie-labs/transfer/lib/dwh/types"
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

	fp, err := s.loadTemporaryTable(tableData, tempTableName)
	if err != nil {
		return fmt.Errorf("failed to load temporary table, err: %v", err)
	}

	// Load fp into s3, get S3 URI and pass it down.
	s3Uri, err := s3.UploadLocalFileToS3(ctx, s3.UploadArgs{
		Bucket:   s.bucket,
		FilePath: fp,
		Expiry:   false,
	})

	if err != nil {
		return fmt.Errorf("failed to upload this to s3, err: %v", err)
	}

	// COPY table_name FROM '/path/to/local/file' DELIMITER '\t' NULL '\\N' FORMAT csv;
	// Note, we need to specify `\\N` here and in `CastColVal(..)` we are only doing `\N`, this is because Redshift treats backslashes as an escape character.
	// So, it'll convert `\N` => `\\N` during COPY.
	copyStmt := fmt.Sprintf(`COPY %s FROM '%s' DELIMITER '\t' NULL AS '\\N' FORMAT CSV %s dateformat 'auto' timeformat 'auto';`, tempTableName, s3Uri, s.credentialsClause)
	fmt.Println(copyStmt)
	if _, err = s.Exec(copyStmt); err != nil {
		return fmt.Errorf("failed to run COPY for temporary table, err: %v, copy: %v", err, copyStmt)
	}
	if deleteErr := os.RemoveAll(fp); deleteErr != nil {
		logger.FromContext(ctx).WithError(deleteErr).WithField("filePath", fp).Warn("failed to delete temp file")
	}

	return nil
}

// loadTemporaryTable will write the data into /tmp/newTableName.csv
// This way, another function can call this and then invoke a Snowflake PUT.
// Returns the file path and potential error
func (s *Store) loadTemporaryTable(tableData *optimization.TableData, newTableName string) (string, error) {
	filePath := fmt.Sprintf("/tmp/%s.csv", newTableName)
	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = '\t'
	for _, value := range tableData.RowsData() {
		var row []string
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(nil) {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal := value[col]
			// Check
			castedValue, castErr := CastColValStaging(colVal, colKind)
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
	return filePath, writer.Error()
}
