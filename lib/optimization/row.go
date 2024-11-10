package optimization

type Row struct {
	primaryKey string
	data       map[string]any
}

func NewRow(primaryKey string, data map[string]any) Row {
	return Row{primaryKey: primaryKey, data: data}
}
