package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/stringutil"

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
	config            config.Config
	uppercaseEscNames bool
}

func (s *Store) Validate() error {
	if s == nil {
		return fmt.Errorf("s3 store is nil")
	}

	if err := s.config.S3.Validate(); err != nil {
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
func (s *Store) ObjectPrefix(tableData *optimization.TableData) string {
	fqTableName := tableData.ToFqName(s.Label(), false, s.uppercaseEscNames, "")
	yyyyMMDDFormat := tableData.LatestCDCTs.Format(ext.PostgresDateFormat)

	if len(s.config.S3.OptionalPrefix) > 0 {
		return strings.Join([]string{s.config.S3.OptionalPrefix, fqTableName, yyyyMMDDFormat}, "/")
	}

	return strings.Join([]string{fqTableName, yyyyMMDDFormat}, "/")
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData) error {
	// There's no difference in appending or merging for S3.
	return s.Merge(ctx, tableData)
}

// Merge - will take tableData, write it into a particular file in the specified format, in these steps:
// 1. Load a ParquetWriter from a JSON schema (auto-generated)
// 2. Load the temporary file, under this format: s3://bucket/optionalS3Prefix/fullyQualifiedTableName/YYYY-MM-DD/{{unix_timestamp}}.parquet.gz
// 3. It will then upload this to S3
// 4. Delete the temporary file
func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
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
		return fmt.Errorf("failed to generate parquet schema, err: %v", err)
	}

	fp := fmt.Sprintf("/tmp/%v_%s.parquet.gz", tableData.LatestCDCTs.UnixMilli(), stringutil.Random(4))
	fw, err := local.NewLocalFileWriter(fp)
	if err != nil {
		return fmt.Errorf("failed to create a local parquet file, err: %v", err)
	}

	pw, err := writer.NewJSONWriter(schema, fw, 4)
	if err != nil {
		return fmt.Errorf("failed to instantiate parquet writer, err: %v", err)
	}

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	pw.CompressionType = parquet.CompressionCodec_GZIP
	for _, val := range tableData.Rows() {
		row := make(map[string]interface{})
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.uppercaseEscNames, nil) {
			colKind, isOk := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			if !isOk {
				return fmt.Errorf("expected column: %v to exist in readOnlyInMemoryCols(...) but it does not", col)
			}

			value, err := parquetutil.ParseValue(val[col], colKind, additionalDateFmts)
			if err != nil {
				return fmt.Errorf("failed to parse value, err: %v, value: %v, column: %v", err, val[col], col)
			}

			row[col] = value
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
		Bucket:                     s.config.S3.Bucket,
		OptionalS3Prefix:           s.ObjectPrefix(tableData),
		FilePath:                   fp,
		OverrideAWSAccessKeyID:     ptr.ToString(s.config.S3.AwsAccessKeyID),
		OverrideAWSAccessKeySecret: ptr.ToString(s.config.S3.AwsSecretAccessKey),
	}); err != nil {
		return fmt.Errorf("failed to upload file to s3, err: %v", err)
	}

	return os.RemoveAll(fp)
}

func LoadStore(cfg config.Config) (*Store, error) {
	store := &Store{
		config:            cfg,
		uppercaseEscNames: false,
	}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	return store, nil
}
