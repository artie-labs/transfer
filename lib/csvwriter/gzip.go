package csvwriter

import (
	"compress/gzip"
	"encoding/csv"
	"os"
)

type GzipWriter struct {
	fp     string
	file   *os.File
	gzip   *gzip.Writer
	writer *csv.Writer
}

func NewGzipWriter(fp string) (*GzipWriter, error) {
	file, err := os.Create(fp)
	if err != nil {
		return nil, err
	}

	return &GzipWriter{
		file:   file,
		fp:     fp,
		gzip:   gzip.NewWriter(file),
		writer: csv.NewWriter(gzip.NewWriter(file)),
	}, nil
}

func (g *GzipWriter) Write(row []string) error {
	return g.writer.Write(row)
}

func (g *GzipWriter) Close() error {
	// Flush the CSV writer
	g.writer.Flush()

	// Then close the gzip writer
	return g.gzip.Close()
}
