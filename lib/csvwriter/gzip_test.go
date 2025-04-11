package csvwriter

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGzipWriter(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "test.csv.gz")
	writer, err := NewGzipWriter(filePath)
	assert.NoError(t, err)

	rows := [][]string{
		{"column1", "column2"},
		{"value1", "value2"},
		{"", ""},                          // Test empty row
		{"hello,dusty", "newline\nvalue"}, // Test special characters
	}

	for _, row := range rows {
		assert.NoError(t, writer.Write(row))
	}

	assert.NoError(t, writer.Flush())
	assert.NoError(t, writer.Close())
	assert.ErrorContains(t, writer.Close(), "already closed")
	assert.Equal(t, "test.csv.gz", writer.FileName())

	// Verify the file contents
	file, err := os.Open(filePath)
	assert.NoError(t, err)
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	assert.NoError(t, err)
	defer gzipReader.Close()

	csvReader := csv.NewReader(gzipReader)
	csvReader.Comma = '\t'

	for _, expectedRow := range rows {
		row, err := csvReader.Read()
		assert.NoError(t, err)
		for j, expectedValue := range expectedRow {
			assert.Equal(t, expectedValue, row[j])
		}
	}
}

func TestGzipWriterLargeData(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "large_test.csv.gz")
	writer, err := NewGzipWriter(filePath)
	assert.NoError(t, err)

	// Test with a large number of rows
	largeRows := make([][]string, 1_000)
	for i := range largeRows {
		largeRows[i] = []string{fmt.Sprintf("value%d", i), fmt.Sprintf("value%d", i)}
	}

	for _, row := range largeRows {
		assert.NoError(t, writer.Write(row))
	}

	assert.NoError(t, writer.Flush())
	assert.NoError(t, writer.Close())
	assert.Equal(t, "large_test.csv.gz", writer.FileName())

	// Verify the file contents
	file, err := os.Open(filePath)
	assert.NoError(t, err)
	defer file.Close()

	gzipReader, err := gzip.NewReader(file)
	assert.NoError(t, err)
	defer gzipReader.Close()

	csvReader := csv.NewReader(gzipReader)
	csvReader.Comma = '\t'

	for _, expectedRow := range largeRows {
		row, err := csvReader.Read()
		assert.NoError(t, err)
		for j, expectedValue := range expectedRow {
			assert.Equal(t, expectedValue, row[j])
		}
	}
}
