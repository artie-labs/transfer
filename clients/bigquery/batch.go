package bigquery

type Batch struct {
	rows        []*Row
	chunkSize   int
	iteratorIdx int
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
