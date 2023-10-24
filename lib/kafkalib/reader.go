package kafkalib

import "github.com/segmentio/kafka-go"

type Reader struct {
	*kafka.Reader
	cfg kafka.ReaderConfig
}

func NewReader(cfg kafka.ReaderConfig) *Reader {
	return &Reader{
		Reader: kafka.NewReader(cfg),
		cfg:    cfg,
	}
}

func (r *Reader) Reload() error {
	// Close the reader first.
	if err := r.Close(); err != nil {
		return err
	}

	// Re-establish the reader connection
	r.Reader = kafka.NewReader(r.cfg)
	return nil
}
