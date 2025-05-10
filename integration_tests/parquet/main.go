package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/artie-labs/transfer/clients/s3"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func main() {
	// Create test data
	cols := &columns.Columns{}
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn("age", typing.Integer))
	cols.AddColumn(columns.NewColumn("created_at", typing.TimestampTZ))

	tableData := optimization.NewTableData(cols, config.Replication, []string{"id"}, kafkalib.TopicConfig{}, "test_table")

	// Add test rows
	tableData.InsertRow("1", map[string]any{
		"id":         1,
		"name":       "John Doe",
		"age":        30,
		"created_at": "2024-03-20T10:00:00Z",
	}, false)
	tableData.InsertRow("2", map[string]any{
		"id":         2,
		"name":       "Jane Smith",
		"age":        25,
		"created_at": "2024-03-20T11:00:00Z",
	}, false)

	// Create output directory if it doesn't exist
	outputDir := "output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("Failed to create output directory: %v\n", err)
		os.Exit(1)
	}

	// Write the parquet file
	parquetPath := filepath.Join(outputDir, "test.parquet")
	if err := s3.WriteParquetFiles(tableData, parquetPath); err != nil {
		fmt.Printf("Failed to write parquet file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Wrote parquet file to %s\n", parquetPath)
}
