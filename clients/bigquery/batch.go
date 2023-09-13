package bigquery

import (
	"fmt"
)

var BatchEmptyErr = fmt.Errorf("batch is empty")

type Batch struct {
	rows        []*Row
	chunkSize   int
	iteratorIdx int
}

func (b *Batch) IsValid() error {
	if len(b.rows) == 0 {
		return BatchEmptyErr
	}

	if b.chunkSize < 1 {
		return fmt.Errorf("chunk size is too small")
	}

	if b.iteratorIdx < 0 {
		return fmt.Errorf("iterator cannot be less than 0")
	}

	return nil
}

func NewBatch(rows []*Row, chunkSize int) *Batch {
	return &Batch{
		rows:      rows,
		chunkSize: chunkSize,
	}
}

func (b *Batch) HasNext() bool {
	return len(b.rows) > b.iteratorIdx
}

func (b *Batch) NextChunk() []*Row {
	start := b.iteratorIdx
	b.iteratorIdx += b.chunkSize
	end := b.iteratorIdx

	if end > len(b.rows) {
		end = len(b.rows)
	}

	if start > end {
		return nil
	}

	return b.rows[start:end]
}
