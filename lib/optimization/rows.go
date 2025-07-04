package optimization

import (
	"time"

	"github.com/artie-labs/transfer/lib/size"
)

type Row struct {
	data map[string]any
	ts   time.Time
}

func NewRow(data map[string]any, ts time.Time) Row {
	return Row{
		data: data,
		ts:   ts,
	}
}

func (r Row) GetValue(key string) (any, bool) {
	val, ok := r.data[key]
	return val, ok
}

func (r Row) GetData() map[string]any {
	return r.data
}

func (r Row) GetApproxSize() int {
	return size.GetApproxSize(r.GetData())
}
