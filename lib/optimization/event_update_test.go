package optimization

import (
	"testing"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestTableData_UpdateInMemoryColumnsFromDestination(t *testing.T) {
	const strCol = "string"

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

	tableDataCols.AddColumn(columns.NewColumn(strCol, typing.String))

	tableData := &TableData{
		inMemoryColumns: tableDataCols,
	}

	nonExistentTableCols := []string{"dusty", "the", "mini", "aussie"}
	var nonExistentCols []columns.Column
	for _, nonExistentTableCol := range nonExistentTableCols {
		nonExistentCols = append(nonExistentCols, columns.NewColumn(nonExistentTableCol, typing.String))
	}

	// Testing to make sure we don't copy over non-existent columns
	tableData.MergeColumnsFromDestination(nonExistentCols...)
	for _, nonExistentTableCol := range nonExistentTableCols {
		_, isOk := tableData.inMemoryColumns.GetColumn(nonExistentTableCol)
		assert.False(t, isOk, nonExistentTableCol)
	}

	// Making sure it's still numeric
	tableData.MergeColumnsFromDestination(columns.NewColumn("numeric_test", typing.Integer))
	numericCol, isOk := tableData.inMemoryColumns.GetColumn("numeric_test")
	assert.True(t, isOk)
	assert.Equal(t, typing.EDecimal.Kind, numericCol.KindDetails.Kind, "numeric_test")

	// Testing to make sure we're copying the kindDetails over.
	tableData.MergeColumnsFromDestination(columns.NewColumn("prev_invalid", typing.String))
	prevInvalidCol, isOk := tableData.inMemoryColumns.GetColumn("prev_invalid")
	assert.True(t, isOk)
	assert.Equal(t, typing.String, prevInvalidCol.KindDetails)

	// Testing backfill
	for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
		assert.False(t, inMemoryCol.Backfilled(), inMemoryCol.RawName())
	}
	backfilledCol := columns.NewColumn("bool_backfill", typing.Boolean)
	backfilledCol.SetBackfilled(true)
	tableData.MergeColumnsFromDestination(backfilledCol)
	for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
		if inMemoryCol.RawName() == backfilledCol.RawName() {
			assert.True(t, inMemoryCol.Backfilled(), inMemoryCol.RawName())
		} else {
			assert.False(t, inMemoryCol.Backfilled(), inMemoryCol.RawName())
		}
	}

	// Testing extTimeDetails
	for _, extTimeDetailsCol := range []string{"ext_date", "ext_time", "ext_datetime"} {
		col, isOk := tableData.inMemoryColumns.GetColumn(extTimeDetailsCol)
		assert.True(t, isOk, extTimeDetailsCol)
		assert.Equal(t, typing.String, col.KindDetails, extTimeDetailsCol)
		assert.Nil(t, col.KindDetails.ExtendedTimeDetails, extTimeDetailsCol)
	}

	tableData.MergeColumnsFromDestination(columns.NewColumn("ext_time", typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType)))
	tableData.MergeColumnsFromDestination(columns.NewColumn("ext_date", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType)))
	tableData.MergeColumnsFromDestination(columns.NewColumn("ext_datetime", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateTimeKindType)))

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

	// Testing extDecimalDetails
	// Confirm that before you update, it's invalid.
	extDecCol, isOk := tableData.inMemoryColumns.GetColumn("ext_dec")
	assert.True(t, isOk)
	assert.Equal(t, typing.String, extDecCol.KindDetails)

	extDecimal := typing.EDecimal
	extDecimal.ExtendedDecimalDetails = decimal.NewDecimal(2, ptr.ToInt(30), nil)
	tableData.MergeColumnsFromDestination(columns.NewColumn("ext_dec", extDecimal))
	// Now it should be ext decimal type
	extDecCol, isOk = tableData.inMemoryColumns.GetColumn("ext_dec")
	assert.True(t, isOk)
	assert.Equal(t, typing.EDecimal.Kind, extDecCol.KindDetails.Kind)
	// Check precision and scale too.
	assert.Equal(t, 30, *extDecCol.KindDetails.ExtendedDecimalDetails.Precision())
	assert.Equal(t, 2, extDecCol.KindDetails.ExtendedDecimalDetails.Scale())

	// Testing ext_dec_filled since it's already filled out
	extDecColFilled, isOk := tableData.inMemoryColumns.GetColumn("ext_dec_filled")
	assert.True(t, isOk)
	assert.Equal(t, typing.EDecimal.Kind, extDecColFilled.KindDetails.Kind)
	// Check precision and scale too.
	assert.Equal(t, 22, *extDecColFilled.KindDetails.ExtendedDecimalDetails.Precision())
	assert.Equal(t, 2, extDecColFilled.KindDetails.ExtendedDecimalDetails.Scale())

	tableData.MergeColumnsFromDestination(columns.NewColumn("ext_dec_filled", extDecimal))
	extDecColFilled, isOk = tableData.inMemoryColumns.GetColumn("ext_dec_filled")
	assert.True(t, isOk)
	assert.Equal(t, typing.EDecimal.Kind, extDecColFilled.KindDetails.Kind)
	// Check precision and scale too.
	assert.Equal(t, 22, *extDecColFilled.KindDetails.ExtendedDecimalDetails.Precision())
	assert.Equal(t, 2, extDecColFilled.KindDetails.ExtendedDecimalDetails.Scale())

	// Testing string precision
	stringKindWithPrecision := typing.KindDetails{
		Kind:                    typing.String.Kind,
		OptionalStringPrecision: ptr.ToInt(123),
	}
	tableData.MergeColumnsFromDestination(columns.NewColumn(strCol, stringKindWithPrecision))
	foundStrCol, isOk := tableData.inMemoryColumns.GetColumn(strCol)
	assert.True(t, isOk)
	assert.Equal(t, typing.String.Kind, foundStrCol.KindDetails.Kind)
	assert.Equal(t, 123, *foundStrCol.KindDetails.OptionalStringPrecision)
}
