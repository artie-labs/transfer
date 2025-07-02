package optimization

func (m MultiStepMergeSettings) IsFirstFlush() bool {
	return m.flushCount == 0
}

func (m MultiStepMergeSettings) IsLastFlush() bool {
	return m.flushCount == m.TotalFlushCount
}

func (m MultiStepMergeSettings) FlushCount() int {
	return m.flushCount
}

func (m *MultiStepMergeSettings) Increment() {
	m.flushCount++
}

type MultiStepMergeSettings struct {
	Enabled         bool
	flushCount      int
	TotalFlushCount int
}

type Row struct {
	originalOp string
	data       map[string]any
}

func NewRow(originalOp string, data map[string]any) Row {
	return Row{
		originalOp: originalOp,
		data:       data,
	}
}

func (r Row) BuildRow(data map[string]any) Row {
	return NewRow(r.originalOp, data)
}
