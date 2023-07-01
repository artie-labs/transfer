package optimization

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestTableData_UpdateInMemoryColumnsFromDestination(t *testing.T) {
	// Test the following scenarios:
	// 4. Test copying `extendedTimeDetails`
	tableDataCols := &columns.Columns{}
	tableDataCols.AddColumn(columns.NewColumn("name", typing.String))
	tableDataCols.AddColumn(columns.NewColumn("bool_backfill", typing.Boolean))
	tableDataCols.AddColumn(columns.NewColumn("prev_invalid", typing.Invalid))
	tableData := &TableData{
		inMemoryColumns: tableDataCols,
	}

	nonExistentTableCols := []string{"dusty", "the", "mini", "aussie"}
	var nonExistentCols []columns.Column
	for _, nonExistentTableCol := range nonExistentTableCols {
		nonExistentCols = append(nonExistentCols, columns.NewColumn(nonExistentTableCol, typing.String))
	}

	// Testing to make sure we don't copy over non-existent columns
	tableData.UpdateInMemoryColumnsFromDestination(nonExistentCols...)
	for _, nonExistentTableCol := range nonExistentTableCols {
		_, isOk := tableData.inMemoryColumns.GetColumn(nonExistentTableCol)
		assert.False(t, isOk, nonExistentTableCol)
	}

	// Testing to make sure we're copying the kindDetails over.
	tableData.UpdateInMemoryColumnsFromDestination(columns.NewColumn("prev_invalid", typing.String))
	prevInvalidCol, isOk := tableData.inMemoryColumns.GetColumn("prev_invalid")
	assert.True(t, isOk)
	assert.Equal(t, typing.String, prevInvalidCol.KindDetails)

	// Testing backfill
	for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
		assert.False(t, inMemoryCol.Backfilled(), inMemoryCol.Name(nil))
	}
	backfilledCol := columns.NewColumn("bool_backfill", typing.Boolean)
	backfilledCol.SetBackfilled(true)
	tableData.UpdateInMemoryColumnsFromDestination(backfilledCol)
	for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
		if inMemoryCol.Name(nil) == backfilledCol.Name(nil) {
			assert.True(t, inMemoryCol.Backfilled(), inMemoryCol.Name(nil))
		} else {
			assert.False(t, inMemoryCol.Backfilled(), inMemoryCol.Name(nil))
		}
	}
}
