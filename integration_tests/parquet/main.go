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

func createComprehensiveTestTable() *optimization.TableData {
	var cols columns.Columns

	// Basic types
	cols.AddColumn(columns.NewColumn("id", typing.Integer))
	cols.AddColumn(columns.NewColumn("name", typing.String))
	cols.AddColumn(columns.NewColumn("age", typing.Integer))
	cols.AddColumn(columns.NewColumn("is_active", typing.Boolean))
	cols.AddColumn(columns.NewColumn("score", typing.Float))

	// Date/Time types
	cols.AddColumn(columns.NewColumn("birth_date", typing.Date))
	cols.AddColumn(columns.NewColumn("lunch_time", typing.Time))
	cols.AddColumn(columns.NewColumn("created_at", typing.TimestampTZ))
	cols.AddColumn(columns.NewColumn("updated_at", typing.TimestampNTZ))

	// Decimal types with different precision/scale
	cols.AddColumn(columns.NewColumn("decimal_small", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(5, 2))))
	cols.AddColumn(columns.NewColumn("decimal_large", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(20, 10))))
	// Use smaller precision for max to stay within 16-byte limit: (precision + 1) / 2 <= 16, so precision <= 31
	cols.AddColumn(columns.NewColumn("decimal_max", typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(30, 15))))

	// Additional string and numeric variations
	cols.AddColumn(columns.NewColumn("description", typing.String))
	cols.AddColumn(columns.NewColumn("big_integer", typing.Integer))
	cols.AddColumn(columns.NewColumn("unicode_text", typing.String))
	cols.AddColumn(columns.NewColumn("empty_string", typing.String))

	// Note: Array and Struct types are not yet fully supported in parquet schema generation
	// They would be converted to JSON strings which is tested with complex_json_string
	cols.AddColumn(columns.NewColumn("complex_json_string", typing.String))

	tableData := optimization.NewTableData(&cols, config.Replication, []string{"id"}, kafkalib.TopicConfig{}, "comprehensive_test_table")
	return tableData
}

func addComprehensiveTestData(tableData *optimization.TableData) {
	// Test row 1: Basic valid data
	tableData.InsertRow("1", map[string]any{
		"id":                  1,
		"name":                "John Doe",
		"age":                 30,
		"is_active":           true,
		"score":               98.5,
		"birth_date":          "1993-05-15",
		"lunch_time":          "12:30:45",
		"created_at":          "2024-03-20T10:00:00.111Z",
		"updated_at":          "2024-03-20T10:00:00.111",
		"decimal_small":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("123.45"), 5),
		"decimal_large":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("1234567890.1234567890"), 20),
		"decimal_max":         decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("123456789012345.123456789012345"), 30),
		"description":         "A premium user from the west coast",
		"big_integer":         9223372036854775807, // max int64
		"unicode_text":        "Hello 世界 🌍 émojis and ünicödé",
		"empty_string":        "",
		"complex_json_string": `{"tags":["user","premium","active"],"metadata":{"country":"US","region":"west","score":100}}`,
	}, false)

	// Test row 2: Edge cases and null-like values
	tableData.InsertRow("2", map[string]any{
		"id":                  2,
		"name":                "Jane Smith",
		"age":                 0, // edge case: zero age
		"is_active":           false,
		"score":               0.0, // edge case: zero score
		"birth_date":          "2000-01-01",
		"lunch_time":          "00:00:00", // edge case: midnight
		"created_at":          "2024-03-20T11:00:00.555Z",
		"updated_at":          "2024-03-20T11:00:00.444",
		"decimal_small":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("0.00"), 5),
		"decimal_large":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-999.9999999999"), 20),
		"decimal_max":         decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-1.000000000000001"), 30),
		"description":         "User with edge case values",
		"big_integer":         -9223372036854775808, // min int64
		"unicode_text":        "Special chars: !@#$%^&*()_+-={}[]|\\:;\"'<>?,./ \t\n",
		"empty_string":        "",
		"complex_json_string": `{"tags":[],"metadata":{}}`,
	}, false)

	// Test row 3: Negative numbers and special values
	tableData.InsertRow("3", map[string]any{
		"id":                  3,
		"name":                "Bob Wilson",
		"age":                 -1, // edge case: negative age (shouldn't happen but testing)
		"is_active":           true,
		"score":               -45.67,
		"birth_date":          "1970-01-01", // Unix epoch
		"lunch_time":          "23:59:59",   // end of day
		"created_at":          "1970-01-01T00:00:00.000Z", // Unix epoch
		"updated_at":          "2099-12-31T23:59:59.999",  // far future
		"decimal_small":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-99.99"), 5),
		"decimal_large":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("999999999.9999999999"), 20),
		"decimal_max":         decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("999999999999999.999999999999999"), 30),
		"description":         "Testing negative values and edge cases",
		"big_integer":         1,
		"unicode_text":        "中文 العربية русский 한국어 日本語",
		"empty_string":        "",
		"complex_json_string": `{"tags":["test","negative","special"],"nested":{"level":2,"test":true}}`,
	}, false)

	// Test row 4: Very long strings and complex nested data
	tableData.InsertRow("4", map[string]any{
		"id":                  4,
		"name":                "Alice Johnson",
		"age":                 25,
		"is_active":           true,
		"score":               75.25,
		"birth_date":          "1999-02-28",               // leap year edge
		"lunch_time":          "12:00:00.123",             // with milliseconds
		"created_at":          "2024-02-29T12:00:00.123Z", // leap year
		"updated_at":          "2024-02-29T12:00:00.123",
		"decimal_small":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("12.34"), 5),
		"decimal_large":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("0.0000000001"), 20),
		"decimal_max":         decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("0.000000000000001"), 30),
		"description":         "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
		"big_integer":         42,
		"unicode_text":        "🎉🎊🎈🎁🎂🍰🍪🍫🍬🍭🍮🍯🍼🥛🍵☕🧃🥤🧋",
		"empty_string":        "",
		"complex_json_string": `{"tags":["looooooooooooooooooooooooooooooooooong","user","test"],"complex":{"nested":{"deep":{"array":[1,"two",3.14,true],"level":4}}},"array_of_objects":[{"id":1,"name":"first"},{"id":2,"name":"second"}]}`,
	}, false)

	// Test row 5: More decimal edge cases
	tableData.InsertRow("5", map[string]any{
		"id":                  5,
		"name":                "Charlie Brown",
		"age":                 999, // large age
		"is_active":           false,
		"score":               100.0,
		"birth_date":          "1900-01-01", // very old date
		"lunch_time":          "01:23:45",
		"created_at":          "2024-12-31T23:59:59.999Z",
		"updated_at":          "2024-12-31T23:59:59.999",
		"decimal_small":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("999.99"), 5),
		"decimal_large":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-1234567890.1234567890"), 20),
		"decimal_max":         decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-99999999999999.999999999999999"), 30),
		"description":         "Testing maximum and minimum decimal values with edge cases",
		"big_integer":         0,
		"unicode_text":        "Mixed: ABC123 αβγ ✓✗ ←→↑↓ ♠♣♥♦",
		"empty_string":        "",
		"complex_json_string": `{"tags":["edge","case","testing","decimal","precision"],"config":{"debug":true,"verbose":false},"version":"1.0"}`,
	}, false)

	// Test row 6: Float precision edge cases
	tableData.InsertRow("6", map[string]any{
		"id":                  6,
		"name":                "Diana Prince",
		"age":                 28,
		"is_active":           true,
		"score":               float32(3.14159265359), // float32 precision test
		"birth_date":          "1996-06-06",
		"lunch_time":          "13:37:42.999", // max milliseconds
		"created_at":          "2000-02-29T00:00:01.001Z", // leap year milliseconds
		"updated_at":          "2000-02-29T00:00:01.001",
		"decimal_small":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("1.01"), 5),
		"decimal_large":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("99999999.9999999999"), 20),
		"decimal_max":         decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("999999999999999.000000000000001"), 30),
		"description":         "Testing float precision and very small decimal differences",
		"big_integer":         1000000000000000000, // large but not max
		"unicode_text":        "Math: π≈3.14159, ∑∞ √∞ ∫∂ ≠≤≥±×÷∙",
		"empty_string":        "",
		"complex_json_string": `{"precision":{"float32":3.14159265359,"double":3.141592653589793},"scientific":{"large":"1.23e+10","small":"1.23e-10"}}`,
	}, false)

	// Test row 7: Date/Time edge cases
	tableData.InsertRow("7", map[string]any{
		"id":                  7,
		"name":                "Eve Adams",
		"age":                 100, // centenarian
		"is_active":           false,
		"score":               0.001, // very small positive
		"birth_date":          "1924-02-29", // leap year century ago
		"lunch_time":          "23:59:59.999", // last millisecond of day
		"created_at":          "2038-01-19T03:14:07.999Z", // near Y2038 problem
		"updated_at":          "2038-01-19T03:14:07.999",
		"decimal_small":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-0.01"), 5),
		"decimal_large":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-0.0000000001"), 20),
		"decimal_max":         decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("-0.000000000000001"), 30),
		"description":         "Testing date/time edge cases and century boundaries",
		"big_integer":         -1000000000000000000, // large negative
		"unicode_text":        "Legacy encoding: ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞßàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ",
		"empty_string":        "",
		"complex_json_string": `{"century":{"year":1924,"leap":true},"millennium":{"y2k":2000,"y2038":2038},"unicode":{"legacy":"ÀÁÂ","modern":"🚀🌟"}}`,
	}, false)

	// Test row 8: Boolean and string edge cases
	tableData.InsertRow("8", map[string]any{
		"id":                  8,
		"name":                "Frank Miller",
		"age":                 1, // infant
		"is_active":           true,
		"score":               -0.0, // negative zero
		"birth_date":          "2023-12-31", // recent date
		"lunch_time":          "00:00:00.001", // first millisecond of day
		"created_at":          "1901-01-01T00:00:00.000Z", // early 20th century
		"updated_at":          "1901-01-01T00:00:00.000",
		"decimal_small":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("99.99"), 5),
		"decimal_large":       decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("1.0000000000"), 20),
		"decimal_max":         decimal.NewDecimalWithPrecision(numbers.MustParseDecimal("1.000000000000000"), 30),
		"description":         "Testing string with quotes \"and\" 'various' `backticks` and [brackets] {braces} <angles>",
		"big_integer":         123456789, // medium sized number
		"unicode_text":        "Code: control chars and spaces     ", // control characters
		"empty_string":        "",
		"complex_json_string": `{"quotes":{"double":"\"hello\"","single":"'world'","backtick":"\`code\`"},"symbols":{"brackets":"[array]","braces":"{object}","angles":"<tag>"}}`,
	}, false)
}

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

	tableData := createComprehensiveTestTable()
	addComprehensiveTestData(tableData)

	// Create output directory if it doesn't exist
	outputDir := "output"
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		logger.Fatal("Failed to create output directory", slog.Any("error", err))
	}

	// Write the parquet file
	parquetPath := filepath.Join(outputDir, "comprehensive_test.parquet")
	if err := s3.WriteParquetFiles(tableData, parquetPath, loc); err != nil {
		logger.Fatal("Failed to write parquet file", slog.Any("error", err))
	}

	slog.Info("Wrote comprehensive parquet file", slog.String("path", parquetPath), slog.Int("rows", len(tableData.Rows())))
}
