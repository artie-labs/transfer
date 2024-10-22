package optimization

import (
	"testing"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
	"github.com/artie-labs/transfer/lib/typing/ext"
	"github.com/stretchr/testify/assert"
)

func TestTableData_UpdateInMemoryColumnsFromDestination_Tz(t *testing.T) {
	{
		// In memory and destination columns are both timestamp_tz
		tableData := &TableData{inMemoryColumns: &columns.Columns{}}
		tableData.AddInMemoryCol(columns.NewColumn("foo", typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, "")))

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("foo", typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, ""))))
		updatedColumn, isOk := tableData.inMemoryColumns.GetColumn("foo")
		assert.True(t, isOk)
		assert.Equal(t, ext.TimestampTZKindType, updatedColumn.KindDetails.ExtendedTimeDetails.Type)
	}
	{
		// In memory is timestamp_ntz and destination is timestamp_tz
		tableData := &TableData{inMemoryColumns: &columns.Columns{}}
		tableData.AddInMemoryCol(
			columns.NewColumn(
				"foo",
				typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampNTZKindType, ext.RFC3339MillisecondNoTZ),
			),
		)

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("foo", typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, ""))))
		updatedColumn, isOk := tableData.inMemoryColumns.GetColumn("foo")
		assert.True(t, isOk)
		assert.Equal(t, ext.TimestampTZKindType, updatedColumn.KindDetails.ExtendedTimeDetails.Type)
		assert.Equal(t, ext.RFC3339Millisecond, updatedColumn.KindDetails.ExtendedTimeDetails.Format)
	}

}

func TestTableData_UpdateInMemoryColumnsFromDestination(t *testing.T) {
	const strCol = "string"
	tableDataCols := &columns.Columns{}
	tableData := &TableData{inMemoryColumns: tableDataCols}
	{
		// Trying to merge an invalid destination column
		tableData.AddInMemoryCol(columns.NewColumn("foo", typing.String))
		invalidCol := columns.NewColumn("foo", typing.Invalid)
		assert.ErrorContains(t, tableData.MergeColumnsFromDestination(invalidCol), `column "foo" is invalid`)
	}
	{
		// In-memory column is a string and the destination column is a Date
		tableData.AddInMemoryCol(columns.NewColumn("foo", typing.String))

		extTime := typing.ETime
		nestedKind, err := ext.NewNestedKind(ext.DateKindType, "")
		assert.NoError(t, err)

		extTime.ExtendedTimeDetails = &nestedKind
		tsCol := columns.NewColumn("foo", extTime)
		assert.NoError(t, tableData.MergeColumnsFromDestination(tsCol))

		updatedColumn, isOk := tableData.inMemoryColumns.GetColumn("foo")
		assert.True(t, isOk)
		assert.Equal(t, typing.ETime.Kind, updatedColumn.KindDetails.Kind)
		assert.Equal(t, ext.DateKindType, updatedColumn.KindDetails.ExtendedTimeDetails.Type)
		// Format is not copied over.
		assert.Equal(t, "", updatedColumn.KindDetails.ExtendedTimeDetails.Format)
	}
	{
		// In-memory column is NUMERIC and destination column is an INTEGER
		tableDataCols.AddColumn(columns.NewColumn("numeric_test", typing.EDecimal))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("numeric_test", typing.Integer)))

		numericCol, isOk := tableData.inMemoryColumns.GetColumn("numeric_test")
		assert.True(t, isOk)
		assert.Equal(t, typing.Integer.Kind, numericCol.KindDetails.Kind)
	}
	{
		// Boolean column that has been backfilled
		tableDataCols.AddColumn(columns.NewColumn("bool_backfill", typing.Boolean))
		backfilledCol := columns.NewColumn("bool_backfill", typing.Boolean)
		backfilledCol.SetBackfilled(true)

		// Backfill was not set
		column, isOk := tableData.inMemoryColumns.GetColumn("bool_backfill")
		assert.True(t, isOk)
		assert.False(t, column.Backfilled())

		assert.NoError(t, tableData.MergeColumnsFromDestination(backfilledCol))
		// Backfill is set after merge.
		column, isOk = tableData.inMemoryColumns.GetColumn("bool_backfill")
		assert.True(t, isOk)
		assert.True(t, column.Backfilled())
	}
	{
		// Non-existent columns should not be copied over.
		nonExistentTableCols := []string{"dusty", "the", "mini", "aussie"}
		var nonExistentCols []columns.Column
		for _, nonExistentTableCol := range nonExistentTableCols {
			nonExistentCols = append(nonExistentCols, columns.NewColumn(nonExistentTableCol, typing.String))
		}

		assert.NoError(t, tableData.MergeColumnsFromDestination(nonExistentCols...))
		for _, nonExistentTableCol := range nonExistentTableCols {
			_, isOk := tableData.inMemoryColumns.GetColumn(nonExistentTableCol)
			assert.False(t, isOk, nonExistentTableCol)
		}
	}
	{
		// In-memory column was invalid, but the destination column is valid
		tableDataCols.AddColumn(columns.NewColumn("invalid_test", typing.Invalid))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("invalid_test", typing.String)))

		invalidCol, isOk := tableData.inMemoryColumns.GetColumn("invalid_test")
		assert.True(t, isOk)
		assert.Equal(t, typing.String.Kind, invalidCol.KindDetails.Kind)
	}
	{
		// Casting these as STRING so tableColumn via this f(x) will set it correctly.
		tableDataCols.AddColumn(columns.NewColumn("ext_date", typing.String))
		tableDataCols.AddColumn(columns.NewColumn("ext_time", typing.String))
		tableDataCols.AddColumn(columns.NewColumn("ext_datetime", typing.String))
		tableDataCols.AddColumn(columns.NewColumn("ext_dec", typing.String))

		extDecimalType := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(22, 2))
		tableDataCols.AddColumn(columns.NewColumn("ext_dec_filled", extDecimalType))
		tableDataCols.AddColumn(columns.NewColumn(strCol, typing.String))

		// Testing extTimeDetails
		for _, extTimeDetailsCol := range []string{"ext_date", "ext_time", "ext_datetime"} {
			col, isOk := tableData.inMemoryColumns.GetColumn(extTimeDetailsCol)
			assert.True(t, isOk, extTimeDetailsCol)
			assert.Equal(t, typing.String, col.KindDetails, extTimeDetailsCol)
			assert.Nil(t, col.KindDetails.ExtendedTimeDetails, extTimeDetailsCol)
		}

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_time", typing.NewExtendedTimeDetails(typing.ETime, ext.TimeKindType, ""))))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_date", typing.NewExtendedTimeDetails(typing.ETime, ext.DateKindType, ""))))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_datetime", typing.NewExtendedTimeDetails(typing.ETime, ext.TimestampTZKindType, ""))))

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
		assert.Equal(t, ext.TimestampTZKindType, dateTimeCol.KindDetails.ExtendedTimeDetails.Type)

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
		// String (precision being copied over)
		tableDataCols.AddColumn(columns.NewColumn(strCol, typing.String))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn(strCol,
			typing.KindDetails{
				Kind:                    typing.String.Kind,
				OptionalStringPrecision: typing.ToPtr(int32(123)),
			}),
		))

		foundStrCol, isOk := tableData.inMemoryColumns.GetColumn(strCol)
		assert.True(t, isOk)
		assert.Equal(t, typing.String.Kind, foundStrCol.KindDetails.Kind)
		assert.Equal(t, int32(123), *foundStrCol.KindDetails.OptionalStringPrecision)
	}
}
