package types

import (
	"log/slog"
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
	case constants.AddColumn:
		for _, col := range cols {
			d.columns.AddColumn(col)
			// Delete from the permissions table, if exists.
			delete(d.columnsToDelete, col.Name())
		}

		// If we're adding columns, then the table should have either been created or already exists.
		d.createTable = false
	case constants.DropColumn:
		for _, col := range cols {
			// Delete from the permissions and in-memory table
			d.columns.DeleteColumn(col.Name())
			delete(d.columnsToDelete, col.Name())
		}
	}
}

func (d *DestinationTableConfig) ShouldDeleteColumn(colName string, cdcTime time.Time, containOtherOperations bool) bool {
	d.Lock()
	defer d.Unlock()

	// We should not delete if either conditions are true.
	// 1. TableData only contains DELETES (delete events may only contain primary key values)
	// 2. Explicit setting that specifies not to drop columns
	if !containOtherOperations || !d.dropDeletedColumns {
		return false
	}

	if ts, ok := d.columnsToDelete[colName]; ok {
		// If the CDC time is greater than this timestamp, then we should delete it.
		return cdcTime.After(ts)
	}

	delTime := time.Now().UTC().Add(constants.DeletionConfidencePadding)
	slog.Info("Column added to columnsToDelete", slog.String("name", colName), slog.Time("deleteAfterTime", delTime))
	d.columnsToDelete[colName] = delTime
	return false
}

// [ColumnMarkedForDeletion] is used to check if a column is marked for deletion, this is used for tests only.
func (d *DestinationTableConfig) ColumnMarkedForDeletion(colName string) bool {
	d.RLock()
	defer d.RUnlock()

	_, ok := d.columnsToDelete[colName]
	return ok
}
