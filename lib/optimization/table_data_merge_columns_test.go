package optimization

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/artie-labs/transfer/lib/typing/decimal"
)

func TestTableData_UpdateInMemoryColumnsFromDestination_Tz(t *testing.T) {
	{
		// In memory and destination columns are both timestamp_tz
		tableData := &TableData{inMemoryColumns: &columns.Columns{}}
		tableData.AddInMemoryCol(columns.NewColumn("foo", typing.TimestampTZ))

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("foo", typing.TimestampTZ)))
		updatedColumn, ok := tableData.inMemoryColumns.GetColumn("foo")
		assert.True(t, ok)
		assert.Equal(t, typing.TimestampTZ, updatedColumn.KindDetails)
	}
	{
		// In memory is timestamp_ntz and destination is timestamp_tz
		tableData := &TableData{inMemoryColumns: &columns.Columns{}}
		tableData.AddInMemoryCol(
			columns.NewColumn(
				"foo",
				typing.TimestampNTZ,
			),
		)

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("foo", typing.TimestampTZ)))
		updatedColumn, ok := tableData.inMemoryColumns.GetColumn("foo")
		assert.True(t, ok)
		assert.Equal(t, typing.TimestampTZ, updatedColumn.KindDetails)
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
		tsCol := columns.NewColumn("foo", typing.Date)
		assert.NoError(t, tableData.MergeColumnsFromDestination(tsCol))

		updatedColumn, ok := tableData.inMemoryColumns.GetColumn("foo")
		assert.True(t, ok)
		assert.Equal(t, typing.Date, updatedColumn.KindDetails)
	}
	{
		// In-memory column is NUMERIC and destination column is an INTEGER
		tableDataCols.AddColumn(columns.NewColumn("numeric_test", typing.EDecimal))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("numeric_test", typing.Integer)))

		numericCol, ok := tableData.inMemoryColumns.GetColumn("numeric_test")
		assert.True(t, ok)
		assert.Equal(t, typing.Integer.Kind, numericCol.KindDetails.Kind)
	}
	{
		// Boolean column that has been backfilled
		tableDataCols.AddColumn(columns.NewColumn("bool_backfill", typing.Boolean))
		backfilledCol := columns.NewColumn("bool_backfill", typing.Boolean)
		backfilledCol.SetBackfilled(true)

		// Backfill was not set
		column, ok := tableData.inMemoryColumns.GetColumn("bool_backfill")
		assert.True(t, ok)
		assert.False(t, column.Backfilled())

		assert.NoError(t, tableData.MergeColumnsFromDestination(backfilledCol))
		// Backfill is set after merge.
		column, ok = tableData.inMemoryColumns.GetColumn("bool_backfill")
		assert.True(t, ok)
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
			_, ok := tableData.inMemoryColumns.GetColumn(nonExistentTableCol)
			assert.False(t, ok, nonExistentTableCol)
		}
	}
	{
		// In-memory column was invalid, but the destination column is valid
		tableDataCols.AddColumn(columns.NewColumn("invalid_test", typing.Invalid))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("invalid_test", typing.String)))

		invalidCol, ok := tableData.inMemoryColumns.GetColumn("invalid_test")
		assert.True(t, ok)
		assert.Equal(t, typing.String.Kind, invalidCol.KindDetails.Kind)
	}
	{
		// Casting these as STRING so tableColumn via this f(x) will set it correctly.
		tableDataCols.AddColumn(columns.NewColumn("ext_dec", typing.String))
		extDecimalType := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(22, 2))
		tableDataCols.AddColumn(columns.NewColumn("ext_dec_filled", extDecimalType))
		tableDataCols.AddColumn(columns.NewColumn(strCol, typing.String))
		{
			// Testing converting from string to various time data types
			{
				// Date
				cols := &columns.Columns{}
				cols.AddColumn(columns.NewColumn("date_column", typing.String))
				td := &TableData{inMemoryColumns: cols}

				assert.NoError(t, td.MergeColumnsFromDestination(columns.NewColumn("date_column", typing.Date)))
				col, ok := td.inMemoryColumns.GetColumn("date_column")
				assert.True(t, ok)
				assert.Equal(t, typing.Date, col.KindDetails)
			}
			{
				// Time
				cols := &columns.Columns{}
				cols.AddColumn(columns.NewColumn("time_column", typing.String))
				td := &TableData{inMemoryColumns: cols}

				assert.NoError(t, td.MergeColumnsFromDestination(columns.NewColumn("time_column", typing.Time)))
				col, ok := td.inMemoryColumns.GetColumn("time_column")
				assert.True(t, ok)
				assert.Equal(t, typing.Time, col.KindDetails)
			}
			{
				// Timestamp TZ
				cols := &columns.Columns{}
				cols.AddColumn(columns.NewColumn("timestamp_tz_column", typing.String))
				td := &TableData{inMemoryColumns: cols}

				assert.NoError(t, td.MergeColumnsFromDestination(columns.NewColumn("timestamp_tz_column", typing.TimestampTZ)))
				col, ok := td.inMemoryColumns.GetColumn("timestamp_tz_column")
				assert.True(t, ok)
				assert.Equal(t, typing.TimestampTZ, col.KindDetails)
			}
		}
		// Testing extDecimalDetails
		// Confirm that before you update, it's invalid.
		extDecCol, ok := tableData.inMemoryColumns.GetColumn("ext_dec")
		assert.True(t, ok)
		assert.Equal(t, typing.String, extDecCol.KindDetails)

		extDecimal := typing.NewDecimalDetailsFromTemplate(typing.EDecimal, decimal.NewDetails(30, 2))
		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_dec", extDecimal)))
		// Now it should be ext decimal type
		extDecCol, ok = tableData.inMemoryColumns.GetColumn("ext_dec")
		assert.True(t, ok)
		assert.Equal(t, typing.EDecimal.Kind, extDecCol.KindDetails.Kind)
		// Check precision and scale too.
		assert.Equal(t, int32(30), extDecCol.KindDetails.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), extDecCol.KindDetails.ExtendedDecimalDetails.Scale())

		// Testing ext_dec_filled since it's already filled out
		extDecColFilled, ok := tableData.inMemoryColumns.GetColumn("ext_dec_filled")
		assert.True(t, ok)
		assert.Equal(t, typing.EDecimal.Kind, extDecColFilled.KindDetails.Kind)
		// Check precision and scale too.
		assert.Equal(t, int32(22), extDecColFilled.KindDetails.ExtendedDecimalDetails.Precision())
		assert.Equal(t, int32(2), extDecColFilled.KindDetails.ExtendedDecimalDetails.Scale())

		assert.NoError(t, tableData.MergeColumnsFromDestination(columns.NewColumn("ext_dec_filled", extDecimal)))
		extDecColFilled, ok = tableData.inMemoryColumns.GetColumn("ext_dec_filled")
		assert.True(t, ok)
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

		foundStrCol, ok := tableData.inMemoryColumns.GetColumn(strCol)
		assert.True(t, ok)
		assert.Equal(t, typing.String.Kind, foundStrCol.KindDetails.Kind)
		assert.Equal(t, int32(123), *foundStrCol.KindDetails.OptionalStringPrecision)
	}
}
