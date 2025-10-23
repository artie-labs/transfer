package shared

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestWriteTemporaryTableFile(t *testing.T) {
	// Create test data
	cols := &columns.Columns{}
	for _, col := range []string{"user_id", "first_name", "last_name", "description"} {
		cols.AddColumn(columns.NewColumn(col, typing.String))
	}

	tableData := optimization.NewTableData(cols, config.Replication, []string{"user_id"}, kafkalib.TopicConfig{}, "test_table")

	// Add test rows
	for i := 0; i < 100; i++ {
		key := fmt.Sprint(i)
		rowData := map[string]any{
			"user_id":     key,
			"first_name":  fmt.Sprintf("First%d", i),
			"last_name":   fmt.Sprintf("Last%d", i),
			"description": "the mini aussie",
		}
		tableData.InsertRow(key, rowData, false)
	}

	// Create a table identifier using Snowflake dialect
	tableID := dialect.NewTableIdentifier("test_db", "test_schema", "test_table")

	// Define a simple value converter
	valueConverter := func(colValue any, colKind typing.KindDetails, _ config.SharedDestinationSettings) (ValueConvertResponse, error) {
		return ValueConvertResponse{Value: fmt.Sprintf("%v", colValue)}, nil
	}

	// Write the temporary table file
	tempTableDataFile := NewTemporaryDataFile(tableID)
	file, _, err := tempTableDataFile.WriteTemporaryTableFile(tableData, valueConverter, config.SharedDestinationSettings{})
	assert.NoError(t, err)
	assert.NotEmpty(t, file.FilePath)
	assert.NotEmpty(t, file.FileName)

	// Read and verify the CSV file
	csvfile, err := os.Open(file.FilePath)
	assert.NoError(t, err)
	defer csvfile.Close()

	gzipReader, err := gzip.NewReader(csvfile)
	assert.NoError(t, err)
	defer gzipReader.Close()

	r := csv.NewReader(gzipReader)
	r.Comma = '\t'

	seenUserID := make(map[string]bool)
	seenFirstName := make(map[string]bool)
	seenLastName := make(map[string]bool)

	for {
		record, readErr := r.Read()
		if readErr == io.EOF {
			break
		}

		assert.NoError(t, readErr)
		assert.Equal(t, 4, len(record))

		seenUserID[record[0]] = true
		seenFirstName[record[1]] = true
		seenLastName[record[2]] = true
		assert.Equal(t, "the mini aussie", record[3])
	}

	assert.Len(t, seenUserID, 100)
	assert.Len(t, seenFirstName, 100)
	assert.Len(t, seenLastName, 100)

	// Clean up
	assert.NoError(t, os.RemoveAll(file.FilePath))
}
