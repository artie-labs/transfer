package types

import (
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/typing"
)

type DwhTableConfig struct {
	// Making these private variables to avoid concurrent R/W panics.

	columns         map[string]typing.Kind
	columnsToDelete map[string]time.Time // column --> when to delete
	CreateTable     bool

	sync.Mutex
}

func NewDwhTableConfig(columns map[string]typing.Kind, colsToDelete map[string]time.Time, createTable bool) *DwhTableConfig {
	return &DwhTableConfig{
		columns:         columns,
		columnsToDelete: colsToDelete,
		CreateTable:     createTable,
	}
}

func (tc *DwhTableConfig) Columns() map[string]typing.Kind {
	return tc.columns
}

func (tc *DwhTableConfig) ColumnsToDelete() map[string]time.Time {
	if tc == nil {
		return nil
	}

	return tc.columnsToDelete
}

func (tc *DwhTableConfig) AddColumnsToDelete(colName string, ts time.Time) {
	tc.Lock()
	defer tc.Unlock()

	if tc.columnsToDelete == nil {
		tc.columnsToDelete = make(map[string]time.Time)
	}

	tc.columnsToDelete[colName] = ts
	return
}

func (tc *DwhTableConfig) ClearColumnsToDeleteByColName(colName string) {
	tc.Lock()
	defer tc.Unlock()

	delete(tc.columnsToDelete, colName)
}
