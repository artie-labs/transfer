package types

import (
	"github.com/artie-labs/transfer/lib/typing"
	"time"
)

type DwhTableConfig struct {
	Columns         map[string]typing.Kind
	ColumnsToDelete map[string]time.Time // column --> when to delete
	CreateTable     bool
}
