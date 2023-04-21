package types

import (
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

type DwhTableConfig struct {
	// Making these private variables to avoid concurrent R/W panics.
	columns         *typing.Columns
	columnsToDelete map[string]time.Time // column --> when to delete
	CreateTable     bool

	// Whether to drop deleted columns in the destination or not.
	dropDeletedColumns bool

	sync.Mutex
}

func NewDwhTableConfig(columns typing.Columns, colsToDelete map[string]time.Time, createTable, dropDeletedColumns bool) *DwhTableConfig {
	if len(colsToDelete) == 0 {
		colsToDelete = make(map[string]time.Time)
	}

	return &DwhTableConfig{
		columns:            &columns,
		columnsToDelete:    colsToDelete,
		CreateTable:        createTable,
		dropDeletedColumns: dropDeletedColumns,
	}
}

func (tc *DwhTableConfig) DropDeletedColumns() bool {
	return tc.dropDeletedColumns
}

func (tc *DwhTableConfig) Columns() typing.Columns {
	// TODO in the future, columns should be wrapped with a type that has mutex support to avoid concurrent r/w panics
	// or consider using sync.Map
	if tc == nil {
		return typing.Columns{}
	}

	return *tc.columns
}

func (tc *DwhTableConfig) MutateInMemoryColumns(createTable bool, columnOp constants.ColumnOperation, cols ...typing.Column) {
	if tc == nil {
		return
	}

	tc.Lock()
	defer tc.Unlock()
	switch columnOp {
	case constants.Add:
		for _, col := range cols {
			tc.columns.AddColumn(col)
			// Delete from the permissions table, if exists.
			delete(tc.columnsToDelete, col.Name)
		}

		tc.CreateTable = createTable
	case constants.Delete:
		for _, col := range cols {
			// Delete from the permissions and in-memory table
			tc.columns.DeleteColumn(col.Name)
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

	if tc.dropDeletedColumns == false {
		// Never delete
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
