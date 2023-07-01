package optimization

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestTableData_UpdateInMemoryColumnsFromDestination(t *testing.T) {
	tableDataCols := &columns.Columns{}
	tableDataCols.AddColumn(columns.NewColumn("name", typing.String))
	tableDataCols.AddColumn(columns.NewColumn("bool_backfill", typing.Boolean))
	tableDataCols.AddColumn(columns.NewColumn("prev_invalid", typing.Invalid))

	// Casting these as STRING so tableColumn via this f(x) will set it correctly.
	tableDataCols.AddColumn(columns.NewColumn("ext_date", typing.String))
	tableDataCols.AddColumn(columns.NewColumn("ext_time", typing.String))
	tableDataCols.AddColumn(columns.NewColumn("ext_datetime", typing.String))
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

	// Testing extTimeDetails
	for _, extTimeDetailsCol := range []string{"ext_date", "ext_time", "ext_datetime"} {
		col, isOk := tableData.inMemoryColumns.GetColumn(extTimeDetailsCol)
		assert.True(t, isOk, extTimeDetailsCol)
		assert.Equal(t, typing.String, col.KindDetails, extTimeDetailsCol)
		assert.Nil(t, col.KindDetails.ExtendedTimeDetails, extTimeDetailsCol)
	}

	tableData.UpdateInMemoryColumnsFromDestination(columns.NewColumn("ext_date", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType)))
	tableData.UpdateInMemoryColumnsFromDestination(columns.NewColumn("ext_time", typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)))
	tableData.UpdateInMemoryColumnsFromDestination(columns.NewColumn("ext_datetime", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)))

	dateCol, isOk := tableData.inMemoryColumns.GetColumn("ext_date")
	assert.True(t, isOk)
	assert.NotNil(t, dateCol.KindDetails.ExtendedTimeDetails)
	assert.Equal(t, ext.DateKindType, dateCol.KindDetails.ExtendedTimeDetails.Type)

	timeCol, isOk := tableData.inMemoryColumns.GetColumn("ext_time")
	assert.True(t, isOk)
	assert.NotNil(t, timeCol.KindDetails.ExtendedTimeDetails)
	assert.Equal(t, ext.TimeKindType, timeCol.KindDetails.ExtendedTimeDetails.Type)

	dateTimeCol, isOk := tableData.inMemoryColumns.GetColumn("ext_datetime")
	assert.True(t, isOk)
	assert.NotNil(t, dateTimeCol.KindDetails.ExtendedTimeDetails)
	assert.Equal(t, ext.DateTimeKindType, dateTimeCol.KindDetails.ExtendedTimeDetails.Type)
}
