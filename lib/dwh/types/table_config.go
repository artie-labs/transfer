package types

import (
	"github.com/artie-labs/transfer/lib/config/constants"
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
	if len(columns) == 0 {
		columns = make(map[string]typing.Kind)
	}

	if len(colsToDelete) == 0 {
		colsToDelete = make(map[string]time.Time)
	}

	return &DwhTableConfig{
		columns:         columns,
		columnsToDelete: colsToDelete,
		CreateTable:     createTable,
	}
}

func (tc *DwhTableConfig) Columns() map[string]typing.Kind {
	if tc == nil {
		return nil
	}

	return tc.columns
}

func (tc *DwhTableConfig) MutateColumnsWithMemCache(createTable bool, columnOp constants.ColumnOperation, cols ...typing.Column) {
	if tc == nil {
		return
	}

	tc.Lock()
	defer tc.Unlock()
	table := tc.columns
	switch columnOp {
	case constants.Add:
		for _, col := range cols {
			table[col.Name] = col.Kind
			// Delete from the permissions table, if exists.
			delete(tc.columnsToDelete, col.Name)
		}

		tc.CreateTable = createTable
	case constants.Delete:
		for _, col := range cols {
			// Delete from the permissions and in-memory table
			delete(table, col.Name)
			delete(tc.columnsToDelete, col.Name)
		}
	}
}

func (tc *DwhTableConfig) ColumnsToDelete() map[string]time.Time {
	if tc == nil {
		return nil
	}

	tc.Lock()
	defer tc.Unlock()

	return tc.columnsToDelete
}

func (tc *DwhTableConfig) ShouldDeleteColumn(colName string, cdcTime time.Time) bool {
	if tc == nil {
		// Avoid a panic and default to FALSE.
		return false
	}

	ts, isOk := tc.ColumnsToDelete()[colName]
	if isOk {
		// If the CDC time is greater than this timestamp, then we should delete it.
		return cdcTime.After(ts)
	}

	tc.AddColumnsToDelete(colName, time.Now().UTC().Add(constants.DeletionConfidencePadding))
	return false
}

func (tc *DwhTableConfig) AddColumnsToDelete(colName string, ts time.Time) {
	if tc == nil {
		return
	}

	tc.Lock()
	defer tc.Unlock()

	if tc.columnsToDelete == nil {
		tc.columnsToDelete = make(map[string]time.Time)
	}

	tc.columnsToDelete[colName] = ts
	return
}

func (tc *DwhTableConfig) ClearColumnsToDeleteByColName(colName string) {
	if tc == nil {
		return
	}

	tc.Lock()
	defer tc.Unlock()

	delete(tc.columnsToDelete, colName)
}
