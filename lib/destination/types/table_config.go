package types

import (
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type DestinationTableConfig struct {
	// Making these private variables to avoid concurrent R/W panics.
	columns         *columns.Columns
	columnsToDelete map[string]time.Time // column --> when to delete
	createTable     bool

	// Whether to drop deleted columns in the destination or not.
	dropDeletedColumns bool
	sync.RWMutex
}

func NewDestinationTableConfig(cols []columns.Column, dropDeletedColumns bool) *DestinationTableConfig {
	return &DestinationTableConfig{
		columns:            columns.NewColumns(cols),
		columnsToDelete:    make(map[string]time.Time),
		createTable:        len(cols) == 0,
		dropDeletedColumns: dropDeletedColumns,
	}
}

func (d *DestinationTableConfig) SetColumnsToDeleteForTest(cols map[string]time.Time) {
	d.Lock()
	defer d.Unlock()

	d.columnsToDelete = cols
}

func (d *DestinationTableConfig) CreateTable() bool {
	d.RLock()
	defer d.RUnlock()

	return d.createTable
}

func (d *DestinationTableConfig) DropDeletedColumns() bool {
	d.RLock()
	defer d.RUnlock()

	return d.dropDeletedColumns
}

func (d *DestinationTableConfig) GetColumns() []columns.Column {
	d.RLock()
	defer d.RUnlock()

	return d.columns.GetColumns()
}

func (d *DestinationTableConfig) UpdateColumn(col columns.Column) {
	d.columns.UpdateColumn(col)
}

func (d *DestinationTableConfig) UpsertColumn(colName string, arg columns.UpsertColumnArg) error {
	return d.columns.UpsertColumn(colName, arg)
}

func (d *DestinationTableConfig) MutateInMemoryColumns(columnOp constants.ColumnOperation, cols ...columns.Column) {
	d.Lock()
	defer d.Unlock()
	switch columnOp {
	case constants.Add:
		for _, col := range cols {
			d.columns.AddColumn(col)
			// Delete from the permissions table, if exists.
			delete(d.columnsToDelete, col.Name())
		}

		// If we're adding columns, then the table should have either been created or already exists.
		d.createTable = false
	case constants.Delete:
		for _, col := range cols {
			// Delete from the permissions and in-memory table
			d.columns.DeleteColumn(col.Name())
			delete(d.columnsToDelete, col.Name())
		}
	}
}

// AuditColumnsToDelete - will check its (*DestinationTableConfig) columnsToDelete against `colsToDelete` and remove any columns that are not in `colsToDelete`.
// `colsToDelete` is derived from diffing the destination and source (if destination has extra columns)
func (d *DestinationTableConfig) AuditColumnsToDelete(colsToDelete []columns.Column) {
	if !d.dropDeletedColumns {
		// If `dropDeletedColumns` is false, then let's skip this.
		return
	}

	d.Lock()
	defer d.Unlock()

	for colName := range d.columnsToDelete {
		var found bool
		for _, col := range colsToDelete {
			if found = col.Name() == colName; found {
				break
			}
		}

		if !found {
			delete(d.columnsToDelete, colName)
		}
	}
}

// ReadOnlyColumnsToDelete returns a read only version of the columns that need to be deleted.
func (d *DestinationTableConfig) ReadOnlyColumnsToDelete() map[string]time.Time {
	d.RLock()
	defer d.RUnlock()
	return maps.Clone(d.columnsToDelete)
}

func (d *DestinationTableConfig) ShouldDeleteColumn(colName string, cdcTime time.Time, containOtherOperations bool) bool {
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
	slog.Info("Column added to columnsToDelete",
		slog.String("colName", colName),
		slog.Time("deleteAfterTime", delTime),
	)

	d.AddColumnsToDelete(colName, delTime)
	return false
}

func (d *DestinationTableConfig) AddColumnsToDelete(colName string, ts time.Time) {
	d.Lock()
	defer d.Unlock()

	if d.columnsToDelete == nil {
		d.columnsToDelete = make(map[string]time.Time)
	}

	d.columnsToDelete[colName] = ts
}
