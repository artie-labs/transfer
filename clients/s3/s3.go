package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/s3lib"

	"github.com/xitongsys/parquet-go/parquet"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/parquetutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
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
	yyyyMMDDFormat := tableData.LatestCDCTs.FOrmat(ext.PostgresDateFormat)

	if len(s.config.S3.FolderName) > 0 {
		return strings.Join([]string{s.config.S3.FolderName, fqTableName, yyyyMMDDFormat}, "/")
	}

	return strings.Join([]string{fqTableName, yyyyMMDDFormat}, "/")
}

func (s *Store) Append(tableData *optimization.TableData, _ bool) error {
	// There's no difference in appending or merging for S3.
	return s.Merge(tableData)
}

// Merge - will take tableData, write it into a particular file in the specified format, in these steps:
// 1. Load a ParquetWriter from a JSON schema (auto-generated)
// 2. Load the temporary file, under this format: s3://bucket/folderName/fullyQualifiedTableName/YYYY-MM-DD/{{unix_timestamp}}.parquet.gz
// 3. It will then upload this to S3
// 4. Delete the temporary file
func (s *Store) Merge(tableData *optimization.TableData) error {
	if tableData.ShouldSkipUpdate() {
		return nil
	}

	var cols []columns.Column
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.KindDetails == typing.Invalid {
			continue
		}

		cols = append(cols, col)
	}

	schema, err := parquetutil.GenerateJSONSchema(cols)
	if err != nil {
		return fmt.Errorf("failed to generate parquet schema: %w", err)
	}

	fp := fmt.Sprintf("/tmp/%v_%s.parquet.gz", tableData.LatestCDCTs.UnixMilli(), stringutil.Random(4))
	fw, err := local.NewLocalFileWriter(fp)
	if err != nil {
		return fmt.Errorf("failed to create a local parquet file: %w", err)
	}

	pw, err := writer.NewJSONWriter(schema, fw, 4)
	if err != nil {
		return fmt.Errorf("failed to instantiate parquet writer: %w", err)
	}

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	pw.CompressionType = parquet.CompressionCodec_GZIP
	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	for _, val := range tableData.Rows() {
		row := make(map[string]any)
		for _, col := range columns {
			value, err := parquetutil.ParseValue(val[col.Name()], col, additionalDateFmts)
			if err != nil {
				return fmt.Errorf("failed to parse value, err: %w, value: %v, column: %q", err, val[col.Name()], col.Name())
			}

			row[col.Name()] = value
		}

		rowBytes, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row: %w", err)
		}

		if err = pw.Write(string(rowBytes)); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to write stop: %w", err)
	}

	if err = fw.Close(); err != nil {
		return fmt.Errorf("failed to close filewriter: %w", err)
	}

	defer func() {
		// Delete the file regardless of outcome to avoid fs build up.
		if removeErr := os.RemoveAll(fp); removeErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", removeErr), slog.String("filePath", fp))
		}
	}()

	if _, err = s3lib.UploadLocalFileToS3(context.Background(), s3lib.UploadArgs{
		Bucket:                     s.config.S3.Bucket,
		OptionalS3Prefix:           s.ObjectPrefix(tableData),
		FilePath:                   fp,
		OverrideAWSAccessKeyID:     ptr.ToString(s.config.S3.AwsAccessKeyID),
		OverrideAWSAccessKeySecret: ptr.ToString(s.config.S3.AwsSecretAccessKey),
	}); err != nil {
		return fmt.Errorf("failed to upload file to s3: %w", err)
	}

	return nil
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
