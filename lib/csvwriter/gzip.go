package csvwriter

import (
	"compress/gzip"
	"encoding/csv"
	"os"
)

type GzipWriter struct {
	file   *os.File
	gzip   *gzip.Writer
	writer *csv.Writer
}

func NewGzipWriter(fp string) (*GzipWriter, error) {
	file, err := os.Create(fp)
	if err != nil {
		return nil, err
	}

	gzipWriter := gzip.NewWriter(file)
	csvWriter := csv.NewWriter(gzipWriter)
	csvWriter.Comma = '\t'
	return &GzipWriter{
		file:   file,
		gzip:   gzipWriter,
		writer: csvWriter,
	}, nil
}

func (g *GzipWriter) Write(row []string) error {
	return g.writer.Write(row)
}

func (g *GzipWriter) Flush() error {
	g.writer.Flush()
	return g.writer.Error()
}

func (g *GzipWriter) Close() error {
	if err := g.gzip.Close(); err != nil {
		// If closing the gzip writer fails, we should still try to close the file.
		_ = g.file.Close()
		return err
	}
	return g.file.Close()
}
