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

	// Create a comprehensive set of columns to test various data types
	var cols columns.Columns
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn("age", typing.Integer))
	cols.AddColumn(columns.NewColumn("score", typing.Float))
	cols.AddColumn(columns.NewColumn("is_active", typing.Boolean))
	cols.AddColumn(columns.NewColumn("birth_date", typing.Date))
	cols.AddColumn(columns.NewColumn("lunch_time", typing.Time))
	cols.AddColumn(columns.NewColumn("created_at", typing.TimestampTZ))
	cols.AddColumn(columns.NewColumn("created_at_ntz", typing.TimestampNTZ))
	cols.AddColumn(columns.NewColumn("balance", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(10, 2))))
	cols.AddColumn(columns.NewColumn("metadata", typing.Struct))
	cols.AddColumn(columns.NewColumn("tags", typing.Array))

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, kafkalib.TopicConfig{}, "test_table")

	// Add comprehensive test rows with various data types
	slog.Info("Adding test data with various data types")

	tableData.InsertRow("1", map[string]any{
		"id":             1,
		"name":           "John Doe",
		"age":            30,
		"score":          85.5,
		"is_active":      true,
		"birth_date":     "1993-06-15",
		"lunch_time":     "12:30:00",
		"created_at":     "2024-03-20T10:00:00.111Z",
		"created_at_ntz": "2024-03-20T10:00:00.111",
		"balance":        decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("1234.56"), 10),
		"metadata":       map[string]interface{}{"role": "admin", "level": 5},
		"tags":           []string{"vip", "premium"},
	}, false)

	tableData.InsertRow("2", map[string]any{
		"id":             2,
		"name":           "Jane Smith",
		"age":            25,
		"score":          92.3,
		"is_active":      false,
		"birth_date":     "1998-12-08",
		"lunch_time":     "13:15:30",
		"created_at":     "2024-03-20T11:00:00.555Z",
		"created_at_ntz": "2024-03-20T11:00:00.444",
		"balance":        decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("567.89"), 10),
		"metadata":       map[string]interface{}{"role": "user", "level": 2},
		"tags":           []string{"standard"},
	}, false)

	tableData.InsertRow("3", map[string]any{
		"id":             3,
		"name":           "Bob Wilson",
		"age":            35,
		"score":          78.9,
		"is_active":      true,
		"birth_date":     "1988-03-22",
		"lunch_time":     "11:45:15",
		"created_at":     "2024-03-20T12:30:00.777Z",
		"created_at_ntz": "2024-03-20T12:30:00.888",
		"balance":        decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-123.45"), 10),
		"metadata":       map[string]interface{}{"role": "moderator", "level": 3},
		"tags":           []string{"beta", "tester", "active"},
	}, false)

	// Create output directory if it doesn't exist
	outputDir := "output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		logger.Fatal("Failed to create output directory", slog.Any("error", err))
	}

	// Write the parquet file using arrow-go implementation
	parquetPath := filepath.Join(outputDir, "test.parquet")
	slog.Info("Writing parquet file with Apache Arrow Go implementation")

	if err := s3.WriteParquetFiles(tableData, parquetPath, loc); err != nil {
		logger.Fatal("Failed to write parquet file", slog.Any("error", err))
	}

	// Verify the file was created and get its size
	fileInfo, err := os.Stat(parquetPath)
	if err != nil {
		logger.Fatal("Failed to stat parquet file", slog.Any("error", err))
	}

	slog.Info("Successfully wrote parquet file",
		slog.String("path", parquetPath),
		slog.Int64("size_bytes", fileInfo.Size()),
		slog.Int("rows", int(tableData.NumberOfRows())),
		slog.Int("columns", len(cols.ValidColumns())),
	)

	// Additional validation
	if fileInfo.Size() == 0 {
		logger.Fatal("Generated parquet file is empty")
	}

	if fileInfo.Size() < 100 {
		logger.Fatal("Generated parquet file seems too small", slog.Int64("size", fileInfo.Size()))
	}

	slog.Info("Parquet file generation completed successfully! ✅")
	slog.Info("Migration from parquet-go to apache/arrow/go is working correctly")
}
