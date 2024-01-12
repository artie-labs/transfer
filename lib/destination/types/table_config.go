package types

import (
	"context"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/typing/columns"
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

func (d *DwhTableConfig) CreateTable() bool {
	d.RLock()
	defer d.RUnlock()

	return d.createTable
}

func (d *DwhTableConfig) DropDeletedColumns() bool {
	d.RLock()
	defer d.RUnlock()

	return d.dropDeletedColumns
}

func (d *DwhTableConfig) Columns() *columns.Columns {
	if d == nil {
		return nil
	}

	return d.columns
}

func (d *DwhTableConfig) MutateInMemoryColumns(ctx context.Context, createTable bool, columnOp constants.ColumnOperation, cols ...columns.Column) {
	d.Lock()
	defer d.Unlock()
	switch columnOp {
	case constants.Add:
		for _, col := range cols {
			d.columns.AddColumn(col)
			// Delete from the permissions table, if exists.
			delete(d.columnsToDelete, col.Name(ctx, nil))
		}

		d.createTable = createTable
	case constants.Delete:
		for _, col := range cols {
			// Delete from the permissions and in-memory table
			d.columns.DeleteColumn(col.Name(ctx, nil))
			delete(d.columnsToDelete, col.Name(ctx, nil))
		}
	}
}

// AuditColumnsToDelete - will check its (*DwhTableConfig) columnsToDelete against `colsToDelete` and remove any columns that are not in `colsToDelete`.
// `colsToDelete` is derived from diffing the destination and source (if destination has extra columns)
func (d *DwhTableConfig) AuditColumnsToDelete(ctx context.Context, colsToDelete []columns.Column) {
	if !d.dropDeletedColumns || len(colsToDelete) == 0 {
		// If `dropDeletedColumns` is false, then let's skip this.
		// Or if there are no `colsToDelete` then let's also skip this since for-loop won't run.
		return
	}

	d.Lock()
	defer d.Unlock()

	for colName := range d.columnsToDelete {
		var found bool
		for _, col := range colsToDelete {
			if found = col.Name(ctx, nil) == colName; found {
				break
			}
		}

		if !found {
			delete(d.columnsToDelete, colName)
		}
	}
}

// ReadOnlyColumnsToDelete returns a read only version of the columns that need to be deleted.
func (d *DwhTableConfig) ReadOnlyColumnsToDelete() map[string]time.Time {
	d.RLock()
	defer d.RUnlock()
	colsToDelete := make(map[string]time.Time)
	for k, v := range d.columnsToDelete {
		colsToDelete[k] = v
	}

	return colsToDelete
}

func (d *DwhTableConfig) ShouldDeleteColumn(ctx context.Context, colName string, cdcTime time.Time, containOtherOperations bool) bool {
	if d == nil {
		// Avoid a panic and default to FALSE.
		return false
	}

	// We should not delete if either conditions are true.
	// 1. TableData contains only DELETES
	// 2. Explicit setting that specifies not to drop columns
	if !containOtherOperations {
		return false
	}

	if !d.dropDeletedColumns {
		return false
	}

	colsToDelete := d.ReadOnlyColumnsToDelete()
	ts, isOk := colsToDelete[colName]
	if isOk {
		// If the CDC time is greater than this timestamp, then we should delete it.
		return cdcTime.After(ts)
	}

	delTime := time.Now().UTC().Add(constants.DeletionConfidencePadding)
	logger.FromContext(ctx).WithFields(map[string]interface{}{
		"colName":         colName,
		"deleteAfterTime": delTime,
	}).Info("column added to columnsToDelete")

	d.AddColumnsToDelete(colName, delTime)
	return false
}

func (d *DwhTableConfig) AddColumnsToDelete(colName string, ts time.Time) {
	d.Lock()
	defer d.Unlock()

	if d.columnsToDelete == nil {
		d.columnsToDelete = make(map[string]time.Time)
	}

	d.columnsToDelete[colName] = ts
}
