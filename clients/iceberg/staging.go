package iceberg

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/converters"
	"github.com/artie-labs/transfer/lib/typing/values"
)

func castColValStaging(colVal any, colKind typing.KindDetails, cfg config.SharedDestinationSettings) (shared.ValueConvertResponse, error) {
	if colVal == nil {
		return shared.ValueConvertResponse{Value: constants.NullValuePlaceholder}, nil
	}

	value, err := values.ToStringOpts(colVal, colKind, converters.GetStringConverterOpts{UseNewStringMethod: cfg.UseNewStringMethod})
	if err != nil {
		return shared.ValueConvertResponse{}, err
	}

	return shared.ValueConvertResponse{Value: value}, nil
}

func (s Store) buildColumnParts(columns []columns.Column) []string {
	var colParts []string
	for _, col := range columns {
		colPart := fmt.Sprintf("%s %s",
			s.Dialect().BuildIdentifier(col.Name()),
			s.Dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), config.SharedDestinationColumnSettings{}),
		)

		colParts = append(colParts, colPart)
	}

	return colParts
}

func (s Store) uploadToS3(ctx context.Context, fp string) (string, error) {
	s3URI, err := s.s3Client.UploadLocalFileToS3(ctx, s.config.Iceberg.S3Tables.Bucket, "", fp)
	if err != nil {
		return "", fmt.Errorf("failed to upload to s3: %w", err)
	}

	// We need to change the prefix from s3:// to s3a://
	// Ref: https://stackoverflow.com/a/33356421
	s3URI = "s3a:" + strings.TrimPrefix(s3URI, "s3:")
	return s3URI, nil
}

func (s *Store) writeTemporaryTableFile(tableData *optimization.TableData, newTableID sql.TableIdentifier) (string, error) {
	tempTableDataFile := shared.NewTemporaryDataFile(newTableID)
	file, _, err := tempTableDataFile.WriteTemporaryTableFile(tableData, castColValStaging, s.config.SharedDestinationSettings, false)
	if err != nil {
		return "", fmt.Errorf("failed to write temporary table file: %w", err)
	}

	return file.FilePath, nil
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

	// Load the data into a temporary view
	command := s.Dialect().BuildCreateTemporaryView(tempTableID.EscapedTable(), s.buildColumnParts(tableData.ReadOnlyInMemoryCols().ValidColumns()), s3URI)
	if err := s.apacheLivyClient.ExecContext(ctx, command); err != nil {
		return fmt.Errorf("failed to load temporary table: %w", err)
	}

	return nil
}
