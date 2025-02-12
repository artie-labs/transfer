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

func NewFilePath(fp string) (*GzipWriter, error) {
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
	if err := g.writer.Error(); err != nil {
		// If the writer failed to close, let's try to close the gzip writer and file.
		_ = g.gzip.Close()
		_ = g.file.Close()
		return err
	}
	if err := g.gzip.Close(); err != nil {
		// If gzip fails, we should at least try to close the file
		_ = g.file.Close()
		return err
	}
	return g.file.Close()
}
