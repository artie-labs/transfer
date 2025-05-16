package main

import (
	"log/slog"
	"os"
	"path/filepath"

	"github.com/artie-labs/transfer/clients/s3"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/numbers"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func main() {
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn("age", typing.Integer))
	cols.AddColumn(columns.NewColumn("created_at", typing.TimestampTZ))
	cols.AddColumn(columns.NewColumn("score", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(10, 7))))

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, kafkalib.TopicConfig{}, "test_table")

	// Add test rows
	tableData.InsertRow("1", map[string]any{
		"id":         1,
		"name":       "John Doe",
		"age":        30,
		"created_at": "2024-03-20T10:00:00Z",
		"score":      decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-97.410511"), 10),
	}, false)
	tableData.InsertRow("2", map[string]any{
		"id":         2,
		"name":       "Jane Smith",
		"age":        25,
		"created_at": "2024-03-20T11:00:00Z",
		"score":      decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("99.410511"), 10),
	}, false)

	// Create output directory if it doesn't exist
	outputDir := "output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		logger.Fatal("Failed to create output directory", slog.Any("error", err))
	}

	// Write the parquet file
	parquetPath := filepath.Join(outputDir, "test.parquet")
	if err := s3.WriteParquetFiles(tableData, parquetPath, nil); err != nil {
		logger.Fatal("Failed to write parquet file", slog.Any("error", err))
	}

	slog.Info("Wrote parquet file", slog.String("path", parquetPath))
}
