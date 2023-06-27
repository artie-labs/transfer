package types

import (
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/config/constants"
)

type DwhTableConfig struct {
	// Making these private variables to avoid concurrent R/W panics.
	columns         *columns.Columns
	columnsToDelete map[string]time.Time // column --> when to delete
	createTable     bool

	// Whether to drop deleted columns in the destination or not.
	dropDeletedColumns bool
	sync.RWMutex
}

func NewDwhTableConfig(columns *columns.Columns, colsToDelete map[string]time.Time, createTable, dropDeletedColumns bool) *DwhTableConfig {
	if len(colsToDelete) == 0 {
		colsToDelete = make(map[string]time.Time)
	}

	return &DwhTableConfig{
		columns:            columns,
		columnsToDelete:    colsToDelete,
		createTable:        createTable,
		dropDeletedColumns: dropDeletedColumns,
	}
}

func (tc *DwhTableConfig) CreateTable() bool {
	tc.RLock()
	defer tc.RUnlock()

	return tc.createTable
}

func (tc *DwhTableConfig) DropDeletedColumns() bool {
	tc.RLock()
	defer tc.RUnlock()

	return tc.dropDeletedColumns
}

func (tc *DwhTableConfig) ReadOnlyColumns() *columns.Columns {
	if tc == nil {
		return nil
	}

	var cols columns.Columns
	for _, col := range tc.columns.GetColumns() {
		cols.AddColumn(col)
	}

	return &cols
}

func (tc *DwhTableConfig) MutateInMemoryColumns(createTable bool, columnOp constants.ColumnOperation, cols ...columns.Column) {
	tc.Lock()
	defer tc.Unlock()
	switch columnOp {
	case constants.Add:
		for _, col := range cols {
			tc.columns.AddColumn(col)
			// Delete from the permissions table, if exists.
			delete(tc.columnsToDelete, col.Name(nil))
		}

		tc.createTable = createTable
	case constants.Delete:
		for _, col := range cols {
			// Delete from the permissions and in-memory table
			tc.columns.DeleteColumn(col.Name(nil))
			delete(tc.columnsToDelete, col.Name(nil))
		}
	}
}

// ReadOnlyColumnsToDelete returns a read only version of the columns that need to be deleted.
func (tc *DwhTableConfig) ReadOnlyColumnsToDelete() map[string]time.Time {
	tc.RLock()
	defer tc.RUnlock()
	colsToDelete := make(map[string]time.Time)
	for k, v := range tc.columnsToDelete {
		colsToDelete[k] = v
	}

	return colsToDelete
}

func (tc *DwhTableConfig) ShouldDeleteColumn(colName string, cdcTime time.Time) bool {
	if tc == nil {
		// Avoid a panic and default to FALSE.
		return false
	}

	if tc.dropDeletedColumns == false {
		// Never delete
		return false
	}

	colsToDelete := tc.ReadOnlyColumnsToDelete()
	ts, isOk := colsToDelete[colName]
	if isOk {
		// If the CDC time is greater than this timestamp, then we should delete it.
		return cdcTime.After(ts)
	}

	tc.AddColumnsToDelete(colName, time.Now().UTC().Add(constants.DeletionConfidencePadding))
	return false
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
