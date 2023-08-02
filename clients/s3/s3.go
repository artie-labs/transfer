package s3

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/artie-labs/transfer/lib/parquetutil"
	"github.com/xitongsys/parquet-go/parquet"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/xitongsys/parquet-go-source/local"

	//"github.com/xitongsys/parquetutil-go/writer"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/optimization"
)

type S3 struct {
	Settings *config.S3Settings
}

func (s *S3) Label() constants.DestinationKind {
	return constants.S3
}

// Merge - will take tableData, write it into a particular file in the specified format.
// It will then upload this file to S3 under this particular format
// s3://bucket/optionalS3Prefix/fullyQualifiedTableName/YYYY-MM-DD/{{unix_timestamp}}.parquetutil.gz
// * fullyQualifiedTableName - databaseName.schemaName.tableName
func (s *S3) Merge(ctx context.Context, tableData *optimization.TableData) error {
	fmt.Println("yay getting called!!!")
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

	fw, err := local.NewLocalFileWriter("/tmp/json.parquet.gz")
	if err != nil {
		return fmt.Errorf("failed to create a local parquetutil file, err: %v", err)
	}

	fmt.Println("schema", schema)

	pw, err := writer.NewJSONWriterFromWriter(fw, schema, 4)
	if err != nil {
		return fmt.Errorf("failed to instantiate parquetutil writer, err: %v", err)
	}

	pw.CompressionType = parquet.CompressionCodec_GZIP
	for _, val := range tableData.RowsData() {
		row := make(map[string]interface{})
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(ctx, nil) {
			//colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			colVal := val[col]

			// TODO parse this.
			row[col] = colVal
		}

		fmt.Println("row", row)
		rowBytes, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("failed to marshal row, err: %v", err)
		}

		fmt.Println("string(rowBytes)", string(rowBytes))
		if err = pw.Write(string(rowBytes)); err != nil {
			return fmt.Errorf("failed to write row, err: %v", err)
		}
	}

	fmt.Println("pw", pw)
	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to write stop, err: %v", err)
	}

	return nil
}

func LoadS3(ctx context.Context, settings *config.S3Settings) *S3 {
	return &S3{
		Settings: settings,
	}
}
