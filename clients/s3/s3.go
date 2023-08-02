//package s3
//
//import (
//	"context"
//	"fmt"
//	"os"
//
//	//"github.com/xitongsys/parquet-go/writer"
//
//	"github.com/artie-labs/transfer/lib/config"
//	"github.com/xitongsys/parquet-go/writer"
//
//	"github.com/artie-labs/transfer/lib/config/constants"
//	"github.com/artie-labs/transfer/lib/optimization"
//)
//
//type S3 struct {
//	Settings *config.S3Settings
//}
//
//func (s *S3) Label() constants.DestinationKind {
//	return constants.S3
//}
//
//// Merge - will take tableData, write it into a particular file in the specified format.
//// It will then upload this file to S3 under this particular format
//// s3://bucket/optionalS3Prefix/fullyQualifiedTableName/YYYY-MM-DD/{{unix_timestamp}}.parquet.gz
//// * fullyQualifiedTableName - databaseName.schemaName.tableName
//func (s *S3) Merge(ctx context.Context, tableData *optimization.TableData) error {
//	if tableData.Rows() == 0 || tableData.ReadOnlyInMemoryCols() == nil {
//		// There's no rows or columns. Let's skip.
//		return nil
//	}
//
//	filename := "/tmp/data.parquet.gz"
//	file, err := os.Create(filename)
//	if err != nil {
//		return err
//	}
//	defer os.Remove(filename) // Ensure the file is deleted when done
//
//	pw, err := writer.NewParquetWriter(file, jsonSchema, 4)
//	if err != nil {
//		return fmt.Errorf("failed to instantiate parquet writer, err: %v", err)
//	}
//
//	for _, val := range tableData.RowsData() {
//		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(ctx, nil) {
//			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
//			colVal := val[col]
//
//			fmt.Println("colKind", colKind, "colVal", colVal)
//		}
//	}
//
//	return nil
//}
//
//func LoadS3(ctx context.Context, settings *config.S3Settings) *S3 {
//	return &S3{
//		Settings: settings,
//	}
//}
