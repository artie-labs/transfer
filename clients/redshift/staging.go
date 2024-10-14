package redshift

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/s3lib"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s *Store) PrepareTemporaryTable(ctx context.Context, tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID sql.TableIdentifier, parentTableID sql.TableIdentifier, _ types.AdditionalSettings, createTempTable bool) error {
	fp, colToNewLengthMap, err := s.loadTemporaryTable(tableData, tempTableID)
	if err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	for colName, newValue := range colToNewLengthMap {
		// Generate queries and then update the [tableConfig] with the new string precision.
		if _, err = s.Exec(s.dialect().BuildIncreaseStringPrecisionQuery(parentTableID, colName, newValue)); err != nil {
			return fmt.Errorf("failed to increase string precision for table %q: %w", parentTableID.FullyQualifiedName(), err)
		}

		tableConfig.Columns().UpsertColumn(colName, columns.UpsertColumnArg{
			StringPrecision: typing.ToPtr(newValue),
		})
	}

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

		if err = tempAlterTableArgs.AlterTable(s, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	defer func() {
		// Remove file regardless of outcome to avoid fs build up.
		if removeErr := os.RemoveAll(fp); removeErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", removeErr), slog.String("filePath", fp))
		}
	}()

	// Load fp into s3, get S3 URI and pass it down.
	s3Uri, err := s3lib.UploadLocalFileToS3(ctx, s3lib.UploadArgs{
		OptionalS3Prefix: s.optionalS3Prefix,
		Bucket:           s.bucket,
		FilePath:         fp,
	})

	if err != nil {
		return fmt.Errorf("failed to upload %q to s3: %w", fp, err)
	}

	// COPY table_name FROM '/path/to/local/file' DELIMITER '\t' NULL '\\N' FORMAT csv;
	// Note, we need to specify `\\N` here and in `CastColVal(..)` we are only doing `\N`, this is because Redshift treats backslashes as an escape character.
	// So, it'll convert `\N` => `\\N` during COPY.
	copyStmt := fmt.Sprintf(
		`COPY %s (%s) FROM '%s' DELIMITER '\t' NULL AS '\\N' GZIP FORMAT CSV %s dateformat 'auto' timeformat 'auto';`,
		tempTableID.FullyQualifiedName(),
		strings.Join(sql.QuoteColumns(tableData.ReadOnlyInMemoryCols().ValidColumns(), s.Dialect()), ","),
		s3Uri,
		s.credentialsClause,
	)

	if _, err = s.Exec(copyStmt); err != nil {
		return fmt.Errorf("failed to run COPY for temporary table: %w", err)
	}

	return nil
}

func (s *Store) loadTemporaryTable(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, map[string]int32, error) {
	filePath := fmt.Sprintf("/tmp/%s.csv.gz", newTableID.FullyQualifiedName())
	file, err := os.Create(filePath)
	if err != nil {
		return "", nil, err
	}

	defer file.Close()
	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	writer := csv.NewWriter(gzipWriter)
	writer.Comma = '\t'
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

		if err = writer.Write(row); err != nil {
			return "", nil, fmt.Errorf("failed to write to csv: %w", err)
		}
	}

	writer.Flush()
	if err = writer.Error(); err != nil {
		return "", nil, fmt.Errorf("failed to flush csv writer: %w", err)
	}

	// This will update the staging columns with the new string precision.
	for colName, newLength := range columnToNewLengthMap {
		tableData.InMemoryColumns().UpsertColumn(colName, columns.UpsertColumnArg{
			StringPrecision: typing.ToPtr(newLength),
		})
	}

	return filePath, columnToNewLengthMap, nil
}
