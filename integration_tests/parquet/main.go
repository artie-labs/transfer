package main

import (
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"time"

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
	var locationString string
	flag.StringVar(&locationString, "location", "", "The location to use for the parquet file")
	flag.Parse()

	var loc *time.Location
	if locationString != "" {
		slog.Info("Loading location", slog.String("location", locationString))
		var err error
		loc, err = time.LoadLocation(locationString)
		if err != nil {
			logger.Fatal("Failed to load location", slog.Any("error", err))
		}
	}

	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn("age", typing.Integer))
	cols.AddColumn(columns.NewColumn("created_at", typing.TimestampTZ))
	cols.AddColumn(columns.NewColumn("created_at_ntz", typing.TimestampNTZ))
	cols.AddColumn(columns.NewColumn("score", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(10, 7))))

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, kafkalib.TopicConfig{}, "test_table")

	// Add test rows
	tableData.InsertRow("1", map[string]any{
		"id":             1,
		"name":           "John Doe",
		"age":            30,
		"created_at":     "2024-03-20T10:00:00.111Z",
		"created_at_ntz": "2024-03-20T10:00:00.111",
		"score":          decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-97.410511"), 10),
	}, false)
	tableData.InsertRow("2", map[string]any{
		"id":             2,
		"name":           "Jane Smith",
		"age":            25,
		"created_at":     "2024-03-20T11:00:00.555Z",
		"created_at_ntz": "2024-03-20T11:00:00.444",
		"score":          decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("99.410511"), 10),
	}, false)

	// Create output directory if it doesn't exist
	outputDir := "output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		logger.Fatal("Failed to create output directory", slog.Any("error", err))
	}

	// Write the parquet file
	parquetPath := filepath.Join(outputDir, "test.parquet")
	if err := s3.WriteParquetFiles(tableData, parquetPath, loc); err != nil {
		logger.Fatal("Failed to write parquet file", slog.Any("error", err))
	}

	slog.Info("Wrote parquet file", slog.String("path", parquetPath))
}
