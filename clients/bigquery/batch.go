package bigquery

type Batch[T any] struct {
	rows        []T
	chunkSize   int
	iteratorIdx int
}

func NewBatch[T any](rows []T, chunkSize int) *Batch[T] {
	return &Batch[T]{
		rows:      rows,
		chunkSize: chunkSize,
	}
}

func (b *Batch[T]) HasNext() bool {
	return len(b.rows) > b.iteratorIdx
}

func (b *Batch[T]) NextChunk() []T {
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
