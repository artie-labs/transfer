package optimization

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
