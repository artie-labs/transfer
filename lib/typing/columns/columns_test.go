package columns

import (
	"fmt"
	"slices"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestEscapeName(t *testing.T) {
	// Test basic name without any transformations
	assert.Equal(t, "foo", EscapeName("foo"))

	// Test uppercase to lowercase conversion
	assert.Equal(t, "fooo", EscapeName("FOOO"))

	// Test spaces being replaced with double underscores
	assert.Equal(t, "col__with__spaces", EscapeName("col with spaces"))

	// Test column name starting with number gets col_ prefix
	assert.Equal(t, "col_1abc", EscapeName("1abc"))
}

func TestColumn_ShouldSkip(t *testing.T) {
	{
		// nil col
		var col *Column
		assert.True(t, col.ShouldSkip())
	}
	{
		// zero col
		var col Column
		assert.True(t, col.ShouldSkip())
	}
	{
		// Invalid col
		col := Column{
			KindDetails: typing.Invalid,
		}
		assert.True(t, col.ShouldSkip())
	}
	{
		// Normal column
		col := Column{KindDetails: typing.String}
		assert.False(t, col.ShouldSkip())
	}
}

func TestColumn_ShouldBackfill(t *testing.T) {
	type _testCase struct {
		name                 string
		column               *Column
		expectShouldBackfill bool
	}

	testCases := []_testCase{
		{
			name: "happy path",
			column: &Column{
				name: "id",
			},
		},
		{
			name: "happy path, primary key",
			column: &Column{
				name:       "id",
				primaryKey: true,
			},
		},
		{
			name: "happy path, primary key (default value set and not backfilled), but since it's a PK - no backfill",
			column: &Column{
				name:         "id",
				primaryKey:   true,
				defaultValue: 123,
			},
		},
		{
			name: "default value set but kind = invalid",
			column: &Column{
				name:         "id",
				defaultValue: "dusty",
				KindDetails:  typing.Invalid,
			},
		},
		{
			name: "default value set but backfilled",
			column: &Column{
				name:         "id",
				defaultValue: "dusty",
				backfilled:   true,
				KindDetails:  typing.String,
			},
		},
		{
			name: "default value set and not backfilled",
			column: &Column{
				name:         "id",
				defaultValue: "dusty",
				KindDetails:  typing.String,
			},
			expectShouldBackfill: true,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectShouldBackfill, testCase.column.ShouldBackfill(), testCase.name)
	}
}

func TestColumns_ValidColumns(t *testing.T) {
	var happyPathCols = []Column{
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

	extraCols := happyPathCols
	for i := 0; i < 100; i++ {
		extraCols = append(extraCols, Column{
			name:        fmt.Sprintf("hello_%v", i),
			KindDetails: typing.Invalid,
		})
	}

	testCases := []struct {
		name         string
		cols         []Column
		expectedCols []Column
	}{
		{
			name:         "happy path",
			cols:         happyPathCols,
			expectedCols: slices.Clone(happyPathCols),
		},
		{
			name:         "happy path + extra col",
			cols:         extraCols,
			expectedCols: slices.Clone(happyPathCols),
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectedCols, (&Columns{columns: testCase.cols}).ValidColumns(), testCase.name)
	}
}

func TestColumns_UpsertColumns(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e"}
	var cols Columns
	for _, key := range keys {
		cols.AddColumn(Column{name: key, KindDetails: typing.String})
	}
	{
		// Now inspect prior to change.
		for _, col := range cols.GetColumns() {
			assert.False(t, col.ToastColumn)
		}
	}
	{
		// Now update a and b to be toast columns
		for _, key := range []string{"a", "b"} {
			assert.NoError(t, cols.UpsertColumn(key, UpsertColumnArg{
				ToastCol: typing.ToPtr(true),
			}))

			// Now inspect.
			col, _ := cols.GetColumn(key)
			assert.True(t, col.ToastColumn)
		}
	}
	{
		// Increase string precision
		{
			// Valid - Current column does not have string precision set
			assert.NoError(t, cols.UpsertColumn("string_precision_a", UpsertColumnArg{}))

			colA, _ := cols.GetColumn("string_precision_a")
			assert.Nil(t, colA.KindDetails.OptionalStringPrecision)

			assert.NoError(t,
				cols.UpsertColumn("string_precision_a",
					UpsertColumnArg{
						StringPrecision: typing.ToPtr(int32(55)),
					},
				),
			)
			colA, _ = cols.GetColumn("string_precision_a")
			assert.Equal(t, int32(55), *colA.KindDetails.OptionalStringPrecision)
		}
		{
			// Valid - Current column does have string precision set (but it's less)
			assert.NoError(t,
				cols.UpsertColumn("string_precision_b",
					UpsertColumnArg{
						StringPrecision: typing.ToPtr(int32(5)),
					},
				),
			)

			colB, _ := cols.GetColumn("string_precision_b")
			assert.Equal(t, int32(5), *colB.KindDetails.OptionalStringPrecision)
			assert.NoError(t,
				cols.UpsertColumn("string_precision_b",
					UpsertColumnArg{
						StringPrecision: typing.ToPtr(int32(100)),
					},
				),
			)

			colB, _ = cols.GetColumn("string_precision_b")
			assert.Equal(t, int32(100), *colB.KindDetails.OptionalStringPrecision)
		}
		{
			// Invalid - Cannot decrease string precision
			assert.NoError(t,
				cols.UpsertColumn("string_precision_b",
					UpsertColumnArg{
						StringPrecision: typing.ToPtr(int32(500)),
					},
				),
			)

			assert.ErrorContains(t,
				cols.UpsertColumn("string_precision_b",
					UpsertColumnArg{
						StringPrecision: typing.ToPtr(int32(100)),
					},
				),
				"cannot decrease string precision from 500 to 100",
			)
		}
	}
	{
		// Create a new column zzz
		assert.NoError(t, cols.UpsertColumn("zzz", UpsertColumnArg{}))
		zzzCol, _ := cols.GetColumn("zzz")
		assert.False(t, zzzCol.ToastColumn)
		assert.False(t, zzzCol.primaryKey)
		assert.Equal(t, zzzCol.KindDetails, typing.Invalid)
	}
	{
		// Create a new column aaa
		assert.NoError(t, cols.UpsertColumn("aaa", UpsertColumnArg{
			ToastCol:   typing.ToPtr(true),
			PrimaryKey: typing.ToPtr(true),
		}))
		aaaCol, _ := cols.GetColumn("aaa")
		assert.True(t, aaaCol.ToastColumn)
		assert.True(t, aaaCol.primaryKey)
		assert.Equal(t, aaaCol.KindDetails, typing.Invalid)
	}
	// Now selectively update only a, b
	for _, key := range []string{"a", "b"} {
		assert.NoError(t, cols.UpsertColumn(key, UpsertColumnArg{
			ToastCol: typing.ToPtr(true),
		}))

		// Now inspect.
		col, _ := cols.GetColumn(key)
		assert.True(t, col.ToastColumn)
	}
	{
		assert.NoError(t, cols.UpsertColumn("zzz", UpsertColumnArg{}))
		zzzCol, _ := cols.GetColumn("zzz")
		assert.False(t, zzzCol.ToastColumn)
		assert.False(t, zzzCol.primaryKey)
		assert.Equal(t, zzzCol.KindDetails, typing.Invalid)
	}
	{
		assert.NoError(t, cols.UpsertColumn("aaa", UpsertColumnArg{
			ToastCol:   typing.ToPtr(true),
			PrimaryKey: typing.ToPtr(true),
		}))

		aaaCol, _ := cols.GetColumn("aaa")
		assert.True(t, aaaCol.ToastColumn)
		assert.True(t, aaaCol.primaryKey)
		assert.Equal(t, aaaCol.KindDetails, typing.Invalid)
	}
	assert.ErrorContains(t, cols.UpsertColumn("", UpsertColumnArg{}), "column name is empty")
}

func TestColumns_Add_Duplicate(t *testing.T) {
	var cols Columns
	duplicateColumns := []Column{{name: "foo"}, {name: "foo"}, {name: "foo"}, {name: "foo"}, {name: "foo"}, {name: "foo"}}
	for _, duplicateColumn := range duplicateColumns {
		cols.AddColumn(duplicateColumn)
	}

	assert.Equal(t, len(cols.GetColumns()), 1, "AddColumn() de-duplicates")
}

func TestColumns_Mutation(t *testing.T) {
	var cols Columns
	colsToAdd := []Column{{name: "foo", KindDetails: typing.String, defaultValue: "bar"}, {name: "bar", KindDetails: typing.Struct}}
	// Insert
	for _, colToAdd := range colsToAdd {
		cols.AddColumn(colToAdd)
	}

	assert.Equal(t, len(cols.GetColumns()), 2)
	fooCol, isOk := cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, typing.String, fooCol.KindDetails)

	barCol, isOk := cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, typing.Struct, barCol.KindDetails)

	// Update
	cols.UpdateColumn(Column{
		name:        "foo",
		KindDetails: typing.Integer,
	})

	cols.UpdateColumn(Column{
		name:         "bar",
		KindDetails:  typing.Boolean,
		defaultValue: "123",
	})

	fooCol, isOk = cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, typing.Integer, fooCol.KindDetails)
	assert.Equal(t, nil, fooCol.defaultValue)

	barCol, isOk = cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, typing.Boolean, barCol.KindDetails)
	assert.Equal(t, "123", barCol.defaultValue)

	// Delete
	cols.DeleteColumn("foo")
	assert.Equal(t, len(cols.GetColumns()), 1)
	cols.DeleteColumn("bar")
	assert.Equal(t, len(cols.GetColumns()), 0)
}

func TestRemoveDeleteColumnMarker(t *testing.T) {
	col1 := NewColumn("a", typing.Invalid)
	col2 := NewColumn("b", typing.Invalid)
	col3 := NewColumn("c", typing.Invalid)
	deleteColumnMarkerCol := NewColumn(constants.DeleteColumnMarker, typing.Invalid)

	{
		_, err := RemoveDeleteColumnMarker([]Column{})
		assert.ErrorContains(t, err, "doesn't exist")
	}
	{
		_, err := RemoveDeleteColumnMarker([]Column{col1})
		assert.ErrorContains(t, err, "doesn't exist")
	}
	{
		_, err := RemoveDeleteColumnMarker([]Column{col1, col2})
		assert.ErrorContains(t, err, "doesn't exist")
	}
	{
		result, err := RemoveDeleteColumnMarker([]Column{deleteColumnMarkerCol})
		assert.NoError(t, err)
		assert.Empty(t, result)
	}
	{
		result, err := RemoveDeleteColumnMarker([]Column{col1, deleteColumnMarkerCol, col2})
		assert.NoError(t, err)
		assert.Equal(t, []Column{col1, col2}, result)
	}
	{
		result, err := RemoveDeleteColumnMarker([]Column{col1, deleteColumnMarkerCol, col2, deleteColumnMarkerCol, col3})
		assert.NoError(t, err)
		assert.Equal(t, []Column{col1, col2, col3}, result)
	}
}

func TestColumnNames(t *testing.T) {
	assert.Empty(t, ColumnNames(nil))

	cols := []Column{
		NewColumn("a", typing.Invalid),
		NewColumn("b", typing.Invalid),
		NewColumn("c", typing.Invalid),
	}
	assert.Equal(t, []string{"a", "b", "c"}, ColumnNames(cols))
}
