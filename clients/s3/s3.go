package s3

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/parquetutil"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

// Parquet writing configuration constants
const (
	// DefaultParquetBatchSize is the default number of rows to process in each batch
	// Smaller batches use less memory but may have slightly lower throughput
	DefaultParquetBatchSize = 1000

	// SmallMemoryBatchSize for memory-constrained environments
	SmallMemoryBatchSize = 500

	// LargeMemoryBatchSize for environments with ample memory
	LargeMemoryBatchSize = 5000
)

type Store struct {
	config   config.Config
	s3Client awslib.S3Client
	location *time.Location
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

// WriteParquetFiles writes the table data to a parquet file at the specified path using Arrow.
// batchSize controls memory usage - smaller values use less memory but may be slower.
// Use 0 for default batch size.
// Returns an error if any step of the writing process fails.
func WriteParquetFiles(tableData *optimization.TableData, filePath string, location *time.Location, batchSize int) error {
	if batchSize <= 0 {
		batchSize = DefaultParquetBatchSize
	}

	arrowSchema, err := parquetutil.BuildArrowSchemaFromColumns(tableData.ReadOnlyInMemoryCols().ValidColumns(), location)
	if err != nil {
		return fmt.Errorf("failed to generate arrow schema: %w", err)
	}

	// Create file for writing
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer file.Close()

	// Create parquet file writer
	writer, err := pqarrow.NewFileWriter(arrowSchema, file, parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Gzip)), pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Use streaming approach to write data in batches
	if err := writeArrowRecordsInBatches(writer, arrowSchema, tableData, location, batchSize); err != nil {
		return fmt.Errorf("failed to write records in batches: %w", err)
	}

	return nil
}

// writeArrowRecordsInBatches processes table data in configurable batch sizes and writes
// Arrow records incrementally to reduce memory usage.
func writeArrowRecordsInBatches(writer *pqarrow.FileWriter, schema *arrow.Schema, tableData *optimization.TableData, location *time.Location, batchSize int) error {
	pool := memory.NewGoAllocator()
	rows := tableData.Rows()
	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()

	// Start a buffered row group to control memory usage
	writer.NewBufferedRowGroup()

	// Process rows in batches
	for batchStart := 0; batchStart < len(rows); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(rows) {
			batchEnd = len(rows)
		}

		// Create builders for this batch
		var builders []array.Builder
		for _, field := range schema.Fields() {
			builders = append(builders, array.NewBuilder(pool, field.Type))
		}

		// Process the current batch of rows
		batchRows := rows[batchStart:batchEnd]
		for _, row := range batchRows {
			for i, col := range cols {
				value, _ := row.GetValue(col.Name())

				// Parse value for Arrow
				parsedValue, err := parquetutil.ParseValueForArrow(value, col.KindDetails, location)
				if err != nil {
					// Clean up builders before returning error
					for _, builder := range builders {
						builder.Release()
					}
					return fmt.Errorf("failed to parse value for column %q: %w", col.Name(), err)
				}

				// Convert and append to builder
				if err := parquetutil.ConvertValueForArrowBuilder(builders[i], parsedValue); err != nil {
					// Clean up builders before returning error
					for _, builder := range builders {
						builder.Release()
					}
					return fmt.Errorf("failed to append value to builder for column %q: %w", col.Name(), err)
				}
			}
		}

		// Build arrays from builders
		arrays := make([]arrow.Array, len(builders))
		for i, builder := range builders {
			arrays[i] = builder.NewArray()
		}

		// Create record for this batch
		record := array.NewRecord(schema, arrays, int64(len(batchRows)))

		// Write the batch to the parquet file using buffered writing
		if err := writer.WriteBuffered(record); err != nil {
			// Clean up before returning error
			record.Release()
			for _, arr := range arrays {
				arr.Release()
			}
			for _, builder := range builders {
				builder.Release()
			}
			return fmt.Errorf("failed to write batch record: %w", err)
		}

		// Clean up this batch's resources
		record.Release()
		for _, arr := range arrays {
			arr.Release()
		}
		for _, builder := range builders {
			builder.Release()
		}
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
	if err := WriteParquetFiles(tableData, fp, s.location, DefaultParquetBatchSize); err != nil {
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

	var location *time.Location
	if cfg.SharedDestinationSettings.SharedTimestampSettings.Location != "" {
		location, err = time.LoadLocation(cfg.SharedDestinationSettings.SharedTimestampSettings.Location)
		if err != nil {
			return nil, fmt.Errorf("failed to load location: %w", err)
		}
	}

	store := Store{
		config:   cfg,
		s3Client: awslib.NewS3Client(awsConfig),
		location: location,
	}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	return &store, nil
}
