package optimization

import (
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func (o *OptimizationTestSuite) TestTableData_UpdateInMemoryColumnsFromDestination() {
	tableDataCols := &columns.Columns{}
	tableDataCols.AddColumn(columns.NewColumn("name", typing.String))
	tableDataCols.AddColumn(columns.NewColumn("bool_backfill", typing.Boolean))
	tableDataCols.AddColumn(columns.NewColumn("prev_invalid", typing.Invalid))
	tableDataCols.AddColumn(columns.NewColumn("numeric_test", typing.EDecimal))

	// Casting these as STRING so tableColumn via this f(x) will set it correctly.
	tableDataCols.AddColumn(columns.NewColumn("ext_date", typing.String))
	tableDataCols.AddColumn(columns.NewColumn("ext_time", typing.String))
	tableDataCols.AddColumn(columns.NewColumn("ext_datetime", typing.String))
	tableDataCols.AddColumn(columns.NewColumn("ext_dec", typing.String))

	extDecimalType := typing.EDecimal
	extDecimalType.ExtendedDecimalDetails = decimal.NewDecimal(2, ptr.ToInt(22), nil)
	tableDataCols.AddColumn(columns.NewColumn("ext_dec_filled", extDecimalType))

	tableData := &TableData{
		inMemoryColumns: tableDataCols,
	}

	nonExistentTableCols := []string{"dusty", "the", "mini", "aussie"}
	var nonExistentCols []columns.Column
	for _, nonExistentTableCol := range nonExistentTableCols {
		nonExistentCols = append(nonExistentCols, columns.NewColumn(nonExistentTableCol, typing.String))
	}

	// Testing to make sure we don't copy over non-existent columns
	tableData.MergeColumnsFromDestination(o.ctx, nonExistentCols...)
	for _, nonExistentTableCol := range nonExistentTableCols {
		_, isOk := tableData.inMemoryColumns.GetColumn(nonExistentTableCol)
		assert.False(o.T(), isOk, nonExistentTableCol)
	}

	// Making sure it's still numeric
	tableData.MergeColumnsFromDestination(o.ctx, columns.NewColumn("numeric_test", typing.Integer))
	numericCol, isOk := tableData.inMemoryColumns.GetColumn("numeric_test")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), typing.EDecimal.Kind, numericCol.KindDetails.Kind, "numeric_test")

	// Testing to make sure we're copying the kindDetails over.
	tableData.MergeColumnsFromDestination(o.ctx, columns.NewColumn("prev_invalid", typing.String))
	prevInvalidCol, isOk := tableData.inMemoryColumns.GetColumn("prev_invalid")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), typing.String, prevInvalidCol.KindDetails)

	// Testing backfill
	for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
		assert.False(o.T(), inMemoryCol.Backfilled(), inMemoryCol.Name(o.ctx, nil))
	}
	backfilledCol := columns.NewColumn("bool_backfill", typing.Boolean)
	backfilledCol.SetBackfilled(true)
	tableData.MergeColumnsFromDestination(o.ctx, backfilledCol)
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

	tableData.MergeColumnsFromDestination(o.ctx, columns.NewColumn("ext_time", typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)))
	tableData.MergeColumnsFromDestination(o.ctx, columns.NewColumn("ext_date", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType)))
	tableData.MergeColumnsFromDestination(o.ctx, columns.NewColumn("ext_datetime", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)))

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

	// Testing extDecimalDetails
	// Confirm that before you update, it's invalid.
	extDecCol, isOk := tableData.inMemoryColumns.GetColumn("ext_dec")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), typing.String, extDecCol.KindDetails)

	extDecimal := typing.EDecimal
	extDecimal.ExtendedDecimalDetails = decimal.NewDecimal(2, ptr.ToInt(30), nil)
	tableData.MergeColumnsFromDestination(o.ctx, columns.NewColumn("ext_dec", extDecimal))
	// Now it should be ext decimal type
	extDecCol, isOk = tableData.inMemoryColumns.GetColumn("ext_dec")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), typing.EDecimal.Kind, extDecCol.KindDetails.Kind)
	// Check precision and scale too.
	assert.Equal(o.T(), 30, *extDecCol.KindDetails.ExtendedDecimalDetails.Precision())
	assert.Equal(o.T(), 2, extDecCol.KindDetails.ExtendedDecimalDetails.Scale())

	// Testing ext_dec_filled since it's already filled out
	extDecColFilled, isOk := tableData.inMemoryColumns.GetColumn("ext_dec_filled")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), typing.EDecimal.Kind, extDecColFilled.KindDetails.Kind)
	// Check precision and scale too.
	assert.Equal(o.T(), 22, *extDecColFilled.KindDetails.ExtendedDecimalDetails.Precision())
	assert.Equal(o.T(), 2, extDecColFilled.KindDetails.ExtendedDecimalDetails.Scale())

	tableData.MergeColumnsFromDestination(o.ctx, columns.NewColumn("ext_dec_filled", extDecimal))
	extDecColFilled, isOk = tableData.inMemoryColumns.GetColumn("ext_dec_filled")
	assert.True(o.T(), isOk)
	assert.Equal(o.T(), typing.EDecimal.Kind, extDecColFilled.KindDetails.Kind)
	// Check precision and scale too.
	assert.Equal(o.T(), 22, *extDecColFilled.KindDetails.ExtendedDecimalDetails.Precision())
	assert.Equal(o.T(), 2, extDecColFilled.KindDetails.ExtendedDecimalDetails.Scale())
}
