package s3

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
)

type Store struct {
	config   config.Config
	s3Client awslib.S3Client
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) Validate() error {
	if err := s.config.S3.Validate(); err != nil {
		return fmt.Errorf("failed to validate settings: %w", err)
	}

	return nil
}

func (s *Store) IdentifierFor(topicConfig kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table, s.config.S3.TableNameSeparator)
}

// ObjectPrefix - this will generate the exact right prefix that we need to write into S3.
// It will look like something like this:
// > folderName/fullyQualifiedTableName/YYYY-MM-DD
func (s *Store) ObjectPrefix(tableData *optimization.TableData) string {
	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	fqTableName := tableID.FullyQualifiedName()
	// Adding date= prefix so that it adheres to the partitioning format for Hive.
	yyyyMMDDFormat := fmt.Sprintf("date=%s", time.Now().Format(time.DateOnly))
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

// WriteParquetFiles writes the table data to a parquet file at the specified path.
// Returns an error if any step of the writing process fails.
func WriteParquetFiles(tableData *optimization.TableData, filePath string) error {
	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	schema, err := parquetutil.BuildCSVSchema(cols)
	if err != nil {
		return fmt.Errorf("failed to generate parquet schema: %w", err)
	}

	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return fmt.Errorf("failed to create a local parquet file: %w", err)
	}

	pw, err := writer.NewCSVWriter(schema, fw, 4)
	if err != nil {
		return fmt.Errorf("failed to instantiate parquet writer: %w", err)
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	for _, row := range tableData.Rows() {
		var csvValues []any
		for _, col := range cols {
			value, _ := row.GetValue(col.Name())
			parsedValue, err := parquetutil.ParseValue(value, col.KindDetails)
			if err != nil {
				return fmt.Errorf("failed to parse value, err: %w, value: %v, column: %q", err, value, col.Name())
			}

			csvValues = append(csvValues, parsedValue)
		}

		if err = pw.Write(csvValues); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to write stop: %w", err)
	}

	if err = fw.Close(); err != nil {
		return fmt.Errorf("failed to close filewriter: %w", err)
	}

	return nil
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

	fp := buildTemporaryFilePath(tableData)
	if err := WriteParquetFiles(tableData, fp); err != nil {
		return false, err
	}

	defer func() {
		// Delete the file regardless of outcome to avoid fs build up.
		if removeErr := os.RemoveAll(fp); removeErr != nil {
			slog.Warn("Failed to delete temp file", slog.Any("err", removeErr), slog.String("filePath", fp))
		}
	}()

	if _, err := s.s3Client.UploadLocalFileToS3(ctx, s.config.S3.Bucket, s.ObjectPrefix(tableData), fp); err != nil {
		return false, fmt.Errorf("failed to upload file to s3: %w", err)
	}

	return true, nil
}

func (s *Store) IsRetryableError(_ error) bool {
	return false // not supported for S3
}

func (s *Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	castedTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("expected tableID to be a TableIdentifier, got %T", tableID)
	}

	return s.s3Client.DeleteFolder(ctx, s.config.S3.Bucket, castedTableID.FullyQualifiedName())
}

func LoadStore(ctx context.Context, cfg config.Config) (*Store, error) {
	creds := credentials.NewStaticCredentialsProvider(cfg.S3.AwsAccessKeyID, cfg.S3.AwsSecretAccessKey, "")
	awsConfig, err := awsCfg.LoadDefaultConfig(ctx, awsCfg.WithCredentialsProvider(creds), awsCfg.WithRegion(cfg.S3.AwsRegion))
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	store := Store{
		config:   cfg,
		s3Client: awslib.NewS3Client(awsConfig),
	}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	return &store, nil
}
