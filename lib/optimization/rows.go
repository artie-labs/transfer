package optimization

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/size"
)

type Row struct {
	initialOp constants.Operation
	currentOp constants.Operation
	data      map[string]any
}

func NewRow(data map[string]any, op constants.Operation) Row {
	return Row{
		data:      data,
		initialOp: op,
	}
}

func (r Row) BuildRow(data map[string]any, op constants.Operation) Row {
	return Row{
		data:      data,
		initialOp: r.initialOp,
		currentOp: op,
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
