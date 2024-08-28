package columns

import (
	"fmt"
	"slices"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
)

func TestEscapeName(t *testing.T) {
	type _testCase struct {
		name         string
		expectedName string
	}

	testCases := []_testCase{
		{
			name:         "foo",
			expectedName: "foo",
		},
		{
			name:         "FOOO",
			expectedName: "fooo",
		},
		{
			name:         "col with spaces",
			expectedName: "col__with__spaces",
		},
	}

	for _, testCase := range testCases {
		actualName := EscapeName(testCase.name)
		assert.Equal(t, testCase.expectedName, actualName, testCase.name)
	}
}

func TestColumn_ShouldSkip(t *testing.T) {
	type _testCase struct {
		name           string
		col            *Column
		expectedResult bool
	}

	testCases := []_testCase{
		{
			name:           "col is nil",
			expectedResult: true,
		},
		{
			name: "invalid column",
			col: &Column{
				SourceKindDetails: typing.Invalid,
			},
			expectedResult: true,
		},
		{
			name: "normal column",
			col: &Column{
				SourceKindDetails: typing.String,
			},
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectedResult, testCase.col.ShouldSkip(), testCase.name)
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
				name:              "id",
				defaultValue:      "dusty",
				SourceKindDetails: typing.Invalid,
			},
		},
		{
			name: "default value set but backfilled",
			column: &Column{
				name:              "id",
				defaultValue:      "dusty",
				backfilled:        true,
				SourceKindDetails: typing.String,
			},
		},
		{
			name: "default value set and not backfilled",
			column: &Column{
				name:              "id",
				defaultValue:      "dusty",
				SourceKindDetails: typing.String,
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
			name:              "hi",
			SourceKindDetails: typing.String,
		},
		{
			name:              "bye",
			SourceKindDetails: typing.String,
		},
		{
			name:              "start",
			SourceKindDetails: typing.String,
		},
	}

	extraCols := happyPathCols
	for i := 0; i < 100; i++ {
		extraCols = append(extraCols, Column{
			name:              fmt.Sprintf("hello_%v", i),
			SourceKindDetails: typing.Invalid,
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
		cols.AddColumn(Column{
			name:              key,
			SourceKindDetails: typing.String,
		})
	}

	// Now inspect prior to change.
	for _, col := range cols.GetColumns() {
		assert.False(t, col.ToastColumn)
	}

	// Now selectively update only a, b
	for _, key := range []string{"a", "b"} {
		cols.UpsertColumn(key, UpsertColumnArg{
			ToastCol: ptr.ToBool(true),
		})

		// Now inspect.
		col, _ := cols.GetColumn(key)
		assert.True(t, col.ToastColumn)
	}

	cols.UpsertColumn("zzz", UpsertColumnArg{})
	zzzCol, _ := cols.GetColumn("zzz")
	assert.False(t, zzzCol.ToastColumn)
	assert.False(t, zzzCol.primaryKey)
	assert.Equal(t, zzzCol.SourceKindDetails, typing.Invalid)

	cols.UpsertColumn("aaa", UpsertColumnArg{
		ToastCol:   ptr.ToBool(true),
		PrimaryKey: ptr.ToBool(true),
	})
	aaaCol, _ := cols.GetColumn("aaa")
	assert.True(t, aaaCol.ToastColumn)
	assert.True(t, aaaCol.primaryKey)
	assert.Equal(t, aaaCol.SourceKindDetails, typing.Invalid)

	length := len(cols.columns)
	for i := 0; i < 500; i++ {
		cols.UpsertColumn("", UpsertColumnArg{})
	}

	assert.Equal(t, length, len(cols.columns))
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
	colsToAdd := []Column{{name: "foo", SourceKindDetails: typing.String, defaultValue: "bar"}, {name: "bar", SourceKindDetails: typing.Struct}}
	// Insert
	for _, colToAdd := range colsToAdd {
		cols.AddColumn(colToAdd)
	}

	assert.Equal(t, len(cols.GetColumns()), 2)
	fooCol, isOk := cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, typing.String, fooCol.SourceKindDetails)

	barCol, isOk := cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, typing.Struct, barCol.SourceKindDetails)

	// Update
	cols.UpdateColumn(Column{
		name:              "foo",
		SourceKindDetails: typing.Integer,
	})

	cols.UpdateColumn(Column{
		name:              "bar",
		SourceKindDetails: typing.Boolean,
		defaultValue:      "123",
	})

	fooCol, isOk = cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, typing.Integer, fooCol.SourceKindDetails)
	assert.Equal(t, nil, fooCol.defaultValue)

	barCol, isOk = cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, typing.Boolean, barCol.SourceKindDetails)
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
