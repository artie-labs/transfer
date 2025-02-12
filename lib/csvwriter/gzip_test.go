package csvwriter

import (
	"compress/gzip"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGzipWriter(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "test.csv.gz")
	writer, err := NewFilePath(filePath)
	assert.NoError(t, err)

	rows := [][]string{
		{"column1", "column2"},
		{"value1", "value2"},
	}

	for _, row := range rows {
		assert.NoError(t, writer.Write(row))
	}

	assert.NoError(t, writer.Close())

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
			if row[j] != expectedValue {
				t.Errorf("expected %s, got %s", expectedValue, row[j])
			}
		}
	}
}
