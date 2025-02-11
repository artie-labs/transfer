package s3

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/parquetutil"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/ext"
)

type Store struct {
	config config.Config
}

func (s *Store) Validate() error {
	if s == nil {
		return fmt.Errorf("s3 store is nil")
	}

	if err := s.config.S3.Validate(); err != nil {
		return fmt.Errorf("failed to validate settings: %w", err)
	}

	return nil
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

// ObjectPrefix - this will generate the exact right prefix that we need to write into S3.
// It will look like something like this:
// > folderName/fullyQualifiedTableName/YYYY-MM-DD
func (s *Store) ObjectPrefix(tableData *optimization.TableData) string {
	tableID := s.IdentifierFor(tableData.TopicConfig(), tableData.Name())
	fqTableName := tableID.FullyQualifiedName()
	// Adding date= prefix so that it adheres to the partitioning format for Hive.
	yyyyMMDDFormat := fmt.Sprintf("date=%s", time.Now().Format(ext.PostgresDateFormat))
	if len(s.config.S3.FolderName) > 0 {
		return strings.Join([]string{s.config.S3.FolderName, fqTableName, yyyyMMDDFormat}, "/")
	}

	return strings.Join([]string{fqTableName, yyyyMMDDFormat}, "/")
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, _ bool) error {
	// There's no difference in appending or merging for S3.
	if _, err := s.Merge(ctx, tableData); err != nil {
		return fmt.Errorf("failed to merge: %w", err)
	}

	return nil
}

func buildTemporaryFilePath(tableData *optimization.TableData) string {
	return fmt.Sprintf("/tmp/%d_%s.parquet", tableData.LatestCDCTs.UnixMilli(), stringutil.Random(4))
}

// Merge - will take tableData, write it into a particular file in the specified format, in these steps:
// 1. Load a ParquetWriter from a JSON schema (auto-generated)
// 2. Load the temporary file, under this format: s3://bucket/folderName/fullyQualifiedTableName/YYYY-MM-DD/{{unix_timestamp}}.parquet
// 3. It will then upload this to S3
// 4. Delete the temporary file
func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	schema, err := parquetutil.BuildCSVSchema(cols)
	if err != nil {
		return false, fmt.Errorf("failed to generate parquet schema: %w", err)
	}

	fp := buildTemporaryFilePath(tableData)
	fw, err := local.NewLocalFileWriter(fp)
	if err != nil {
		return false, fmt.Errorf("failed to create a local parquet file: %w", err)
	}

	pw, err := writer.NewCSVWriter(schema, fw, 4)
	if err != nil {
		return false, fmt.Errorf("failed to instantiate parquet writer: %w", err)
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	for _, val := range tableData.Rows() {
		var row []any
		for _, col := range cols {
			value, err := parquetutil.ParseValue(val[col.Name()], col.KindDetails)
			if err != nil {
				return false, fmt.Errorf("failed to parse value, err: %w, value: %v, column: %q", err, val[col.Name()], col.Name())
			}

			row = append(row, value)
		}

		if err = pw.Write(row); err != nil {
			return false, fmt.Errorf("failed to write row: %w", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return false, fmt.Errorf("failed to write stop: %w", err)
	}

	if err = fw.Close(); err != nil {
		return false, fmt.Errorf("failed to close filewriter: %w", err)
	}

	defer func() {
		// Delete the file regardless of outcome to avoid fs build up.
		if removeErr := os.RemoveAll(fp); removeErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", removeErr), slog.String("filePath", fp))
		}
	}()

	if _, err = awslib.UploadLocalFileToS3(ctx, awslib.UploadArgs{
		Bucket:                     s.config.S3.Bucket,
		OptionalS3Prefix:           s.ObjectPrefix(tableData),
		FilePath:                   fp,
		OverrideAWSAccessKeyID:     typing.ToPtr(s.config.S3.AwsAccessKeyID),
		OverrideAWSAccessKeySecret: typing.ToPtr(s.config.S3.AwsSecretAccessKey),
	}); err != nil {
		return false, fmt.Errorf("failed to upload file to s3: %w", err)
	}

	return true, nil
}

func (s *Store) IsRetryableError(_ error) bool {
	return false // not supported for S3
}

func LoadStore(cfg config.Config) (*Store, error) {
	store := &Store{config: cfg}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	return store, nil
}
