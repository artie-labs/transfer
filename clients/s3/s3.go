package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/artie-labs/transfer/lib/typing/ext"

	"github.com/artie-labs/transfer/lib/s3lib"

	"github.com/xitongsys/parquet-go/parquet"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/parquetutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type Store struct {
	Settings *config.S3Settings
}

func (s *Store) Validate() error {
	if s == nil {
		return fmt.Errorf("s3 store is nil")
	}

	if err := s.Settings.Validate(); err != nil {
		return fmt.Errorf("failed to validate settings, err :%v", err)
	}

	return nil
}

func (s *Store) Label() constants.DestinationKind {
	return constants.S3
}

// ObjectPrefix - this will generate the exact right prefix that we need to write into S3.
// It will look like something like this:
// > optionalPrefix/fullyQualifiedTableName/YYYY-MM-DD
func (s *Store) ObjectPrefix(ctx context.Context, tableData *optimization.TableData) string {
	fqTableName := tableData.ToFqName(ctx, s.Label(), false)
	yyyyMMDDFormat := tableData.LatestCDCTs.Format(ext.PostgresDateFormat)

	if len(s.Settings.OptionalPrefix) > 0 {
		return strings.Join([]string{s.Settings.OptionalPrefix, fqTableName, yyyyMMDDFormat}, "/")
	}

	return strings.Join([]string{fqTableName, yyyyMMDDFormat}, "/")
}

// Merge - will take tableData, write it into a particular file in the specified format, in these steps:
// 1. Load a ParquetWriter from a JSON schema (auto-generated)
// 2. Load the temporary file, under this format: s3://bucket/optionalS3Prefix/fullyQualifiedTableName/YYYY-MM-DD/{{unix_timestamp}}.parquet.gz
// 3. It will then upload this to S3
// 4. Delete the temporary file
func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
		// There's no rows or columns. Let's skip.
		return nil
	}

	var cols []columns.Column
	for _, col := range tableData.ReadOnlyInMemoryCols().GetColumns() {
		if col.KindDetails == typing.Invalid {
			continue
		}

		cols = append(cols, col)
	}

	schema, err := parquetutil.GenerateJSONSchema(ctx, cols)
	if err != nil {
		return fmt.Errorf("failed to generate parquetutil schema, err: %v", err)
	}

	fp := fmt.Sprintf("/tmp/%v.parquet.gz", tableData.LatestCDCTs.UnixMilli())
	fw, err := local.NewLocalFileWriter(fp)
	if err != nil {
		return fmt.Errorf("failed to create a local parquetutil file, err: %v", err)
	}

	pw, err := writer.NewJSONWriter(schema, fw, 4)
	if err != nil {
		return fmt.Errorf("failed to instantiate parquetutil writer, err: %v", err)
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	for _, val := range tableData.RowsData() {
		row := make(map[string]interface{})
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(ctx, nil) {
			colVal := val[col]
			row[col] = colVal
		}

		rowBytes, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row, err: %v", err)
		}

		if err = pw.Write(string(rowBytes)); err != nil {
			return fmt.Errorf("failed to write row, err: %v", err)
		}
	}

	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to write stop, err: %v", err)
	}

	if err = fw.Close(); err != nil {
		return fmt.Errorf("failed to close filewriter, err: %v", err)
	}

	if _, err = s3lib.UploadLocalFileToS3(ctx, s3lib.UploadArgs{
		Bucket:           s.Settings.Bucket,
		OptionalS3Prefix: s.ObjectPrefix(ctx, tableData),
		FilePath:         fp,
	}); err != nil {
		return fmt.Errorf("failed to upload file to s3, err: %v", err)
	}

	return os.RemoveAll(fp)
}

func LoadStore(ctx context.Context, settings *config.S3Settings) (*Store, error) {
	store := &Store{
		Settings: settings,
	}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	return store, nil
}
