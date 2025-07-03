package optimization

import "github.com/artie-labs/transfer/lib/size"

type Row struct {
	data map[string]any
}

func NewRow(data map[string]any) Row {
	return Row{
		data: data,
	}
}

func (r Row) GetValue(key string) (any, bool) {
	val, ok := r.data[key]
	return val, ok
}

func (r Row) GetData() map[string]any {
	return r.data
}

func (r *Row) SetValue(key string, value any) {
	r.data[key] = value
}

func (r Row) GetApproxSize() int {
	return size.GetApproxSize(r.GetData())
}
