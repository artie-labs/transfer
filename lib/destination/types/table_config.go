package types

import (
	"log/slog"
	"maps"
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
	d.columnsToDelete = cols
}

func (d *DestinationTableConfig) CreateTable() bool {
	return d.createTable
}

func (d *DestinationTableConfig) DropDeletedColumns() bool {
	return d.dropDeletedColumns
}

func (d *DestinationTableConfig) GetColumns() []columns.Column {
	return d.columns.GetColumns()
}

func (d *DestinationTableConfig) UpdateColumn(col columns.Column) {
	d.columns.UpdateColumn(col)
}

func (d *DestinationTableConfig) UpsertColumn(colName string, arg columns.UpsertColumnArg) error {
	return d.columns.UpsertColumn(colName, arg)
}

func (d *DestinationTableConfig) MutateInMemoryColumns(columnOp constants.ColumnOperation, cols ...columns.Column) {
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

// ReadOnlyColumnsToDelete returns a read only version of the columns that need to be deleted.
func (d *DestinationTableConfig) ReadOnlyColumnsToDelete() map[string]time.Time {
	return maps.Clone(d.columnsToDelete)
}

func (d *DestinationTableConfig) ShouldDeleteColumn(colName string, cdcTime time.Time, containOtherOperations bool) bool {
	// We should not delete if any of these conditions are true:
	// 1. TableData only contains deletes (delete events may only contain the primary key values)
	// 2. If dropping columns is disabled
	if !containOtherOperations || !d.dropDeletedColumns {
		return false
	}

	colsToDelete := d.ReadOnlyColumnsToDelete()
	if ts, ok := colsToDelete[colName]; ok {
		// If the CDC time is greater than this timestamp, then we should delete it.
		return cdcTime.After(ts)
	}

	delTime := time.Now().UTC().Add(constants.DeletionConfidencePadding)
	slog.Info("Column added to columnsToDelete", slog.String("name", colName), slog.Time("deleteAfterTime", delTime))

	d.AddColumnsToDelete(colName, delTime)
	return false
}

func (d *DestinationTableConfig) AddColumnsToDelete(colName string, ts time.Time) {
	d.columnsToDelete[colName] = ts
}
