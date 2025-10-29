package columns

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
)

func TestEscapeName(t *testing.T) {
	expected := map[string]string{
		"foo":             "foo",
		"FOOO":            "fooo",
		"col with spaces": "col__with__spaces",
		"1abc":            "col_1abc",
		"bar#baz":         "bar__baz",
		"case":            "col_case",
	}

	for input, expected := range expected {
		assert.Equal(t, expected, EscapeName(input, map[string]bool{"case": true}))
	}
}

func TestColumn_ShouldSkip(t *testing.T) {
	{
		// Test when column is nil
		var col *Column
		assert.True(t, col.ShouldSkip())
	}
	{
		// Test when column is invalid
		col := Column{
			KindDetails: typing.Invalid,
		}
		assert.True(t, col.ShouldSkip())
	}
	{
		// Test normal column
		col := Column{KindDetails: typing.String}
		assert.False(t, col.ShouldSkip())
	}
}

func TestColumn_ShouldBackfill(t *testing.T) {
	{
		// Test happy path with basic column
		col := &Column{
			name: "id",
		}
		assert.False(t, col.ShouldBackfill())
	}
	{
		// Test primary key column
		col := &Column{
			name:       "id",
			primaryKey: true,
		}
		assert.False(t, col.ShouldBackfill())
	}
	{
		// Test primary key with default value
		col := &Column{
			name:         "id",
			primaryKey:   true,
			defaultValue: 123,
		}
		assert.False(t, col.ShouldBackfill())
	}
	{
		// Test invalid column with default value
		col := &Column{
			name:         "id",
			defaultValue: "dusty",
			KindDetails:  typing.Invalid,
		}
		assert.False(t, col.ShouldBackfill())
	}
	{
		// Test already backfilled column
		col := &Column{
			name:         "id",
			defaultValue: "dusty",
			backfilled:   true,
			KindDetails:  typing.String,
		}
		assert.False(t, col.ShouldBackfill())
	}
	{
		// Test column that needs backfilling
		col := &Column{
			name:         "id",
			defaultValue: "dusty",
			KindDetails:  typing.String,
		}
		assert.True(t, col.ShouldBackfill())
	}
}

func TestColumns_ValidColumns(t *testing.T) {
	{
		// Setup test columns
		happyPathCols := []Column{
			{
				name:        "hi",
				KindDetails: typing.String,
			},
			{
				name:        "bye",
				KindDetails: typing.String,
			},
			{
				name:        "start",
				KindDetails: typing.String,
			},
		}

		// Test happy path with valid columns
		assert.Equal(t, happyPathCols, (&Columns{columns: happyPathCols}).ValidColumns())
	}
	{
		// Test with mix of valid and invalid columns
		happyPathCols := []Column{
			{
				name:        "hi",
				KindDetails: typing.String,
			},
			{
				name:        "bye",
				KindDetails: typing.String,
			},
			{
				name:        "start",
				KindDetails: typing.String,
			},
		}

		extraCols := slices.Clone(happyPathCols)
		for i := 0; i < 100; i++ {
			extraCols = append(extraCols, Column{
				name:        fmt.Sprintf("hello_%v", i),
				KindDetails: typing.Invalid,
			})
		}

		assert.Equal(t, happyPathCols, (&Columns{columns: extraCols}).ValidColumns())
	}
}

func TestColumns_UpsertColumns(t *testing.T) {
	{
		// Setup initial columns
		keys := []string{"a", "b", "c", "d", "e"}
		var cols Columns
		for _, key := range keys {
			cols.AddColumn(Column{name: key, KindDetails: typing.String})
		}

		// Verify initial state
		for _, col := range cols.GetColumns() {
			assert.False(t, col.ToastColumn)
		}
	}
	{
		// Test updating toast columns
		var cols Columns
		cols.AddColumn(Column{name: "a", KindDetails: typing.String})
		cols.AddColumn(Column{name: "b", KindDetails: typing.String})

		for _, key := range []string{"a", "b"} {
			assert.NoError(t, cols.UpsertColumn(key, UpsertColumnArg{
				ToastCol: typing.ToPtr(true),
			}))

			col, _ := cols.GetColumn(key)
			assert.True(t, col.ToastColumn)
		}
	}
	{
		// Test string precision updates
		var cols Columns

		// Test when no precision is set initially
		assert.NoError(t, cols.UpsertColumn("string_precision_a", UpsertColumnArg{}))
		colA, _ := cols.GetColumn("string_precision_a")
		assert.Nil(t, colA.KindDetails.OptionalStringPrecision)

		// Test setting initial precision
		assert.NoError(t, cols.UpsertColumn("string_precision_a", UpsertColumnArg{
			StringPrecision: typing.ToPtr(int32(55)),
		}))
		colA, _ = cols.GetColumn("string_precision_a")
		assert.Equal(t, int32(55), *colA.KindDetails.OptionalStringPrecision)

		// Test increasing precision
		assert.NoError(t, cols.UpsertColumn("string_precision_b", UpsertColumnArg{
			StringPrecision: typing.ToPtr(int32(5)),
		}))
		assert.NoError(t, cols.UpsertColumn("string_precision_b", UpsertColumnArg{
			StringPrecision: typing.ToPtr(int32(100)),
		}))
		colB, _ := cols.GetColumn("string_precision_b")
		assert.Equal(t, int32(100), *colB.KindDetails.OptionalStringPrecision)

		// Test decreasing precision (should fail)
		assert.NoError(t, cols.UpsertColumn("string_precision_b", UpsertColumnArg{
			StringPrecision: typing.ToPtr(int32(500)),
		}))
		assert.ErrorContains(t, cols.UpsertColumn("string_precision_b", UpsertColumnArg{
			StringPrecision: typing.ToPtr(int32(100)),
		}), "cannot decrease string precision from 500 to 100")
	}
	{
		// Test creating new columns
		var cols Columns

		// Test creating basic column
		assert.NoError(t, cols.UpsertColumn("zzz", UpsertColumnArg{}))
		zzzCol, _ := cols.GetColumn("zzz")
		assert.False(t, zzzCol.ToastColumn)
		assert.False(t, zzzCol.primaryKey)
		assert.Equal(t, zzzCol.KindDetails, typing.Invalid)

		// Test creating column with toast and primary key
		assert.NoError(t, cols.UpsertColumn("aaa", UpsertColumnArg{
			ToastCol:   typing.ToPtr(true),
			PrimaryKey: typing.ToPtr(true),
		}))
		aaaCol, _ := cols.GetColumn("aaa")
		assert.True(t, aaaCol.ToastColumn)
		assert.True(t, aaaCol.primaryKey)
		assert.Equal(t, aaaCol.KindDetails, typing.Invalid)
	}
	{
		// Test empty column name
		var cols Columns
		assert.ErrorContains(t, cols.UpsertColumn("", UpsertColumnArg{}), "column name is empty")
	}
}

func TestColumns_Add_Duplicate(t *testing.T) {
	{
		// Test adding duplicate columns
		var cols Columns
		duplicateColumns := []Column{{name: "foo"}, {name: "foo"}, {name: "foo"}, {name: "foo"}, {name: "foo"}, {name: "foo"}}
		for _, duplicateColumn := range duplicateColumns {
			cols.AddColumn(duplicateColumn)
		}

		assert.Equal(t, len(cols.GetColumns()), 1, "AddColumn() de-duplicates")
	}
}

func TestColumns_Mutation(t *testing.T) {
	{
		// Test column insertion
		var cols Columns
		colsToAdd := []Column{
			{name: "foo", KindDetails: typing.String, defaultValue: "bar"},
			{name: "bar", KindDetails: typing.Struct},
		}

		for _, colToAdd := range colsToAdd {
			cols.AddColumn(colToAdd)
		}

		assert.Equal(t, len(cols.GetColumns()), 2)

		fooCol, ok := cols.GetColumn("foo")
		assert.True(t, ok)
		assert.Equal(t, typing.String, fooCol.KindDetails)

		barCol, ok := cols.GetColumn("bar")
		assert.True(t, ok)
		assert.Equal(t, typing.Struct, barCol.KindDetails)
	}
	{
		// Test column updates
		var cols Columns
		cols.AddColumn(Column{name: "foo", KindDetails: typing.String, defaultValue: "bar"})
		cols.AddColumn(Column{name: "bar", KindDetails: typing.Struct})

		cols.UpdateColumn(Column{
			name:        "foo",
			KindDetails: typing.Integer,
		})

		cols.UpdateColumn(Column{
			name:         "bar",
			KindDetails:  typing.Boolean,
			defaultValue: "123",
		})

		fooCol, ok := cols.GetColumn("foo")
		assert.True(t, ok)
		assert.Equal(t, typing.Integer, fooCol.KindDetails)
		assert.Equal(t, nil, fooCol.defaultValue)

		barCol, ok := cols.GetColumn("bar")
		assert.True(t, ok)
		assert.Equal(t, typing.Boolean, barCol.KindDetails)
		assert.Equal(t, "123", barCol.defaultValue)
	}
	{
		// Test column deletion
		var cols Columns
		cols.AddColumn(Column{name: "foo", KindDetails: typing.String})
		cols.AddColumn(Column{name: "bar", KindDetails: typing.String})

		cols.DeleteColumn("foo")
		assert.Equal(t, len(cols.GetColumns()), 1)

		cols.DeleteColumn("bar")
		assert.Equal(t, len(cols.GetColumns()), 0)
	}
}

func TestRemoveDeleteColumnMarker(t *testing.T) {
	col1 := NewColumn("a", typing.Invalid)
	col2 := NewColumn("b", typing.Invalid)
	col3 := NewColumn("c", typing.Invalid)
	deleteColumnMarkerCol := NewColumn(constants.DeleteColumnMarker, typing.Invalid)

	{
		// Test empty column list
		_, err := RemoveDeleteColumnMarker([]Column{})
		assert.ErrorContains(t, err, "doesn't exist")
	}
	{
		// Test single column without marker
		_, err := RemoveDeleteColumnMarker([]Column{col1})
		assert.ErrorContains(t, err, "doesn't exist")
	}
	{
		// Test multiple columns without marker
		_, err := RemoveDeleteColumnMarker([]Column{col1, col2})
		assert.ErrorContains(t, err, "doesn't exist")
	}
	{
		// Test only marker column
		result, err := RemoveDeleteColumnMarker([]Column{deleteColumnMarkerCol})
		assert.NoError(t, err)
		assert.Empty(t, result)
	}
	{
		// Test marker between columns
		result, err := RemoveDeleteColumnMarker([]Column{col1, deleteColumnMarkerCol, col2})
		assert.NoError(t, err)
		assert.Equal(t, []Column{col1, col2}, result)
	}
	{
		// Test multiple markers
		result, err := RemoveDeleteColumnMarker([]Column{col1, deleteColumnMarkerCol, col2, deleteColumnMarkerCol, col3})
		assert.NoError(t, err)
		assert.Equal(t, []Column{col1, col2, col3}, result)
	}
}

func TestColumnNames(t *testing.T) {
	{
		// Test nil input
		assert.Empty(t, ColumnNames(nil))
	}
	{
		// Test normal columns
		cols := []Column{
			NewColumn("a", typing.Invalid),
			NewColumn("b", typing.Invalid),
			NewColumn("c", typing.Invalid),
		}
		assert.Equal(t, []string{"a", "b", "c"}, ColumnNames(cols))
	}
}
