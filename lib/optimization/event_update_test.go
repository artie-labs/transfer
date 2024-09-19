package optimization

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

const strCol = "string"

func TestTableData_UpdateInMemoryColumnsFromDestination(t *testing.T) {
	{
		tableDataCols := &columns.Columns{}
		tableData := &TableData{
			inMemoryColumns: tableDataCols,
		}

		tableData.AddInMemoryCol(columns.NewColumn("foo", typing.String))
		invalidCol := columns.NewColumn("foo", typing.Invalid)
		assert.ErrorContains(t, tableData.MergeColumnsFromDestination(invalidCol), `column "foo" is invalid`)
	}
	{
		// If the in-memory column is a string and the destination column is Date
		// We should mark the in-memory column as date and try to parse it accordingly.
		tableDataCols := &columns.Columns{}
		tableData := &TableData{
			inMemoryColumns: tableDataCols,
		}

		tableData.AddInMemoryCol(columns.NewColumn("foo", typing.String))

		extTime := typing.ETime
		extTime.ExtendedTimeDetails = &ext.NestedKind{
			Type: ext.DateKindType,
		}

		tsCol := columns.NewColumn("foo", extTime)
		assert.NoError(t, tableData.MergeColumnsFromDestination(tsCol))

		col, isOk := tableData.inMemoryColumns.GetColumn("foo")
		assert.True(t, isOk)
		assert.Equal(t, typing.ETime.Kind, col.KindDetails.Kind)
		assert.Equal(t, ext.DateKindType, col.KindDetails.ExtendedTimeDetails.Type)
		assert.Equal(t, extTime.ExtendedTimeDetails, col.KindDetails.ExtendedTimeDetails)
	}
	{
		tableDataCols := &columns.Columns{}
		tableData := &TableData{
			inMemoryColumns: tableDataCols,
		}

		tableDataCols.AddColumn(columns.NewColumn("name", typing.String))
		tableDataCols.AddColumn(columns.NewColumn("bool_backfill", typing.Boolean))
		tableDataCols.AddColumn(columns.NewColumn("prev_invalid", typing.Invalid))
		tableDataCols.AddColumn(columns.NewColumn("numeric_test", typing.EDecimal))

		// Casting these as STRING so tableColumn via this f(x) will set it correctly.
		tableDataCols.AddColumn(columns.NewColumn("ext_date", typing.String))
		tableDataCols.AddColumn(columns.NewColumn("ext_time", typing.String))
		tableDataCols.AddColumn(columns.NewColumn("ext_datetime", typing.String))
		tableDataCols.AddColumn(columns.NewColumn("ext_dec", typing.String))

		extDecimalType := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(22, 2))
		tableDataCols.AddColumn(columns.NewColumn("ext_dec_filled", extDecimalType))

		tableDataCols.AddColumn(columns.NewColumn(strCol, typing.String))

		nonExistentTableCols := []string{"dusty", "the", "mini", "aussie"}
		var nonExistentCols []columns.Column
		for _, nonExistentTableCol := range nonExistentTableCols {
			nonExistentCols = append(nonExistentCols, columns.NewColumn(nonExistentTableCol, typing.String))
		}

		// Testing to make sure we don't copy over non-existent columns
		assert.NoError(t, tableData.MergeColumnsFromDestination(nonExistentCols...))
		for _, nonExistentTableCol := range nonExistentTableCols {
			_, isOk := tableData.inMemoryColumns.GetColumn(nonExistentTableCol)
			assert.False(t, isOk, nonExistentTableCol)
		}

		// Making sure it's still numeric
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("numeric_test", typing.Integer)))
		numericCol, isOk := tableData.inMemoryColumns.GetColumn("numeric_test")
		assert.True(t, isOk)
		assert.Equal(t, typing.EDecimal.Kind, numericCol.KindDetails.Kind, "numeric_test")

		// Testing to make sure we're copying the kindDetails over.
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("prev_invalid", typing.String)))
		prevInvalidCol, isOk := tableData.inMemoryColumns.GetColumn("prev_invalid")
		assert.True(t, isOk)
		assert.Equal(t, typing.String, prevInvalidCol.KindDetails)

		// Testing backfill
		for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
			assert.False(t, inMemoryCol.Backfilled(), inMemoryCol.Name())
		}
		backfilledCol := columns.NewColumn("bool_backfill", typing.Boolean)
		backfilledCol.SetBackfilled(true)
		assert.NoError(t, tableData.MergeColumnsFromDestination(backfilledCol))
		for _, inMemoryCol := range tableData.inMemoryColumns.GetColumns() {
			if inMemoryCol.Name() == backfilledCol.Name() {
				assert.True(t, inMemoryCol.Backfilled(), inMemoryCol.Name())
			} else {
				assert.False(t, inMemoryCol.Backfilled(), inMemoryCol.Name())
			}
		}

		// Testing extTimeDetails
		for _, extTimeDetailsCol := range []string{"ext_date", "ext_time", "ext_datetime"} {
			col, isOk := tableData.inMemoryColumns.GetColumn(extTimeDetailsCol)
			assert.True(t, isOk, extTimeDetailsCol)
			assert.Equal(t, typing.String, col.KindDetails, extTimeDetailsCol)
			assert.Nil(t, col.KindDetails.ExtendedTimeDetails, extTimeDetailsCol)
		}

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_time", typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimeKindType))))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_date", typing.NewKindDetailsFromTemplate(typing.ETime, ext.DateKindType))))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_datetime", typing.NewKindDetailsFromTemplate(typing.ETime, ext.TimestampTzKindType))))

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
		assert.Equal(t, ext.TimestampTzKindType, dateTimeCol.KindDetails.ExtendedTimeDetails.Type)

		// Testing extDecimalDetails
		// Confirm that before you update, it's invalid.
		extDecCol, isOk := tableData.inMemoryColumns.GetColumn("ext_dec")
		assert.True(t, isOk)
		assert.Equal(t, typing.String, extDecCol.KindDetails)

		extDecimal := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(30, 2))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_dec", extDecimal)))
		// Now it should be ext decimal type
		extDecCol, isOk = tableData.inMemoryColumns.GetColumn("ext_dec")
		assert.True(t, isOk)
		assert.Equal(t, typing.EDecimal.Kind, extDecCol.KindDetails.Kind)
		// Check precision and scale too.
		assert.Equal(t, int32(30), extDecCol.KindDetails.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), extDecCol.KindDetails.ExtendedDecimalDetails.Scale())

		// Testing ext_dec_filled since it's already filled out
		extDecColFilled, isOk := tableData.inMemoryColumns.GetColumn("ext_dec_filled")
		assert.True(t, isOk)
		assert.Equal(t, typing.EDecimal.Kind, extDecColFilled.KindDetails.Kind)
		// Check precision and scale too.
		assert.Equal(t, int32(22), extDecColFilled.KindDetails.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), extDecColFilled.KindDetails.ExtendedDecimalDetails.Scale())

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_dec_filled", extDecimal)))
		extDecColFilled, isOk = tableData.inMemoryColumns.GetColumn("ext_dec_filled")
		assert.True(t, isOk)
		assert.Equal(t, typing.EDecimal.Kind, extDecColFilled.KindDetails.Kind)
		// Check precision and scale too.
		assert.Equal(t, int32(22), extDecColFilled.KindDetails.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), extDecColFilled.KindDetails.ExtendedDecimalDetails.Scale())
	}
	{
		tableDataCols := &columns.Columns{}
		tableData := &TableData{
			inMemoryColumns: tableDataCols,
		}

		tableDataCols.AddColumn(columns.NewColumn(strCol, typing.String))

		// Testing string precision
		stringKindWithPrecision := typing.KindDetails{
			Kind:                    typing.String.Kind,
			OptionalStringPrecision: typing.ToPtr(int32(123)),
		}

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn(strCol, stringKindWithPrecision)))
		foundStrCol, isOk := tableData.inMemoryColumns.GetColumn(strCol)
		assert.True(t, isOk)
		assert.Equal(t, typing.String.Kind, foundStrCol.KindDetails.Kind)
		assert.Equal(t, int32(123), *foundStrCol.KindDetails.OptionalStringPrecision)
	}
}
