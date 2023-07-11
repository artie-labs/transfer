package optimization

import (
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func (o *OptimizationTestSuite) TestTableData_UpdateInMemoryColumnsFromDestination() {
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
	tableData.UpdateInMemoryColumnsFromDestination(o.ctx, nonExistentCols...)
	for _, nonExistentTableCol := range nonExistentTableCols {
		_, isOk := tableData.inMemoryColumns.GetColumn(nonExistentTableCol)
		assert.False(o.T(), isOk, nonExistentTableCol)
	}

	// Testing to make sure we're copying the kindDetails over.
	tableData.UpdateInMemoryColumnsFromDestination(o.ctx, columns.NewColumn("prev_invalid", typing.String))
	prevInvalidCol, isOk := tableData.inMemoryColumns.GetColumn("prev_invalid")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), typing.String, prevInvalidCol.KindDetails)

	// Testing backfill
	for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
		assert.False(o.T(), inMemoryCol.Backfilled(), inMemoryCol.Name(o.ctx, nil))
	}
	backfilledCol := columns.NewColumn("bool_backfill", typing.Boolean)
	backfilledCol.SetBackfilled(true)
	tableData.UpdateInMemoryColumnsFromDestination(o.ctx, backfilledCol)
	for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
		if inMemoryCol.Name(o.ctx, nil) == backfilledCol.Name(o.ctx, nil) {
			assert.True(o.T(), inMemoryCol.Backfilled(), inMemoryCol.Name(o.ctx, nil))
		} else {
			assert.False(o.T(), inMemoryCol.Backfilled(), inMemoryCol.Name(o.ctx, nil))
		}
	}

	// Testing extTimeDetails
	for _, extTimeDetailsCol := range []string{"ext_date", "ext_time", "ext_datetime"} {
		col, isOk := tableData.inMemoryColumns.GetColumn(extTimeDetailsCol)
		assert.True(o.T(), isOk, extTimeDetailsCol)
		assert.Equal(o.T(), typing.String, col.KindDetails, extTimeDetailsCol)
		assert.Nil(o.T(), col.KindDetails.ExtendedTimeDetails, extTimeDetailsCol)
	}

	tableData.UpdateInMemoryColumnsFromDestination(o.ctx, columns.NewColumn("ext_date", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType)))
	tableData.UpdateInMemoryColumnsFromDestination(o.ctx, columns.NewColumn("ext_time", typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)))
	tableData.UpdateInMemoryColumnsFromDestination(o.ctx, columns.NewColumn("ext_datetime", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)))

	dateCol, isOk := tableData.inMemoryColumns.GetColumn("ext_date")
	assert.True(o.T(), isOk)
	assert.NotNil(o.T(), dateCol.KindDetails.ExtendedTimeDetails)
	assert.Equal(o.T(), ext.DateKindType, dateCol.KindDetails.ExtendedTimeDetails.Type)

	timeCol, isOk := tableData.inMemoryColumns.GetColumn("ext_time")
	assert.True(o.T(), isOk)
	assert.NotNil(o.T(), timeCol.KindDetails.ExtendedTimeDetails)
	assert.Equal(o.T(), ext.TimeKindType, timeCol.KindDetails.ExtendedTimeDetails.Type)

	dateTimeCol, isOk := tableData.inMemoryColumns.GetColumn("ext_datetime")
	assert.True(o.T(), isOk)
	assert.NotNil(o.T(), dateTimeCol.KindDetails.ExtendedTimeDetails)
	assert.Equal(o.T(), ext.DateTimeKindType, dateTimeCol.KindDetails.ExtendedTimeDetails.Type)
}
