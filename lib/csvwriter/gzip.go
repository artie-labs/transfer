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

func (g *GzipWriter) Close() error {
	g.writer.Flush()
	if err := g.writer.Error(); err != nil {
		return err
	}
	if err := g.gzip.Close(); err != nil {
		return err
	}
	return g.file.Close()
}
