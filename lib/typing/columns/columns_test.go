package columns

import (
	"fmt"
	"testing"

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
				KindDetails: typing.Invalid,
			},
			expectedResult: true,
		},
		{
			name: "normal column",
			col: &Column{
				KindDetails: typing.String,
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

func TestColumns_GetColumnsToUpdate(t *testing.T) {
	type _testCase struct {
		name         string
		cols         []Column
		expectedCols []string
	}

	var (
		happyPathCols = []Column{
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
	)

	extraCols := happyPathCols
	for i := 0; i < 100; i++ {
		extraCols = append(extraCols, Column{
			name:        fmt.Sprintf("hello_%v", i),
			KindDetails: typing.Invalid,
		})
	}

	testCases := []_testCase{
		{
			name:         "happy path",
			cols:         happyPathCols,
			expectedCols: []string{"hi", "bye", "start"},
		},
		{
			name:         "happy path + extra col",
			cols:         extraCols,
			expectedCols: []string{"hi", "bye", "start"},
		},
	}

	for _, testCase := range testCases {
		cols := &Columns{
			columns: testCase.cols,
		}

		assert.Equal(t, testCase.expectedCols, cols.GetColumnsToUpdate(), testCase.name)
	}
}

func TestColumns_UpsertColumns(t *testing.T) {
	keys := []string{"a", "b", "c", "d", "e"}
	var cols Columns
	for _, key := range keys {
		cols.AddColumn(Column{
			name:        key,
			KindDetails: typing.String,
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
	assert.Equal(t, zzzCol.KindDetails, typing.Invalid)

	cols.UpsertColumn("aaa", UpsertColumnArg{
		ToastCol:   ptr.ToBool(true),
		PrimaryKey: ptr.ToBool(true),
	})
	aaaCol, _ := cols.GetColumn("aaa")
	assert.True(t, aaaCol.ToastColumn)
	assert.True(t, aaaCol.primaryKey)
	assert.Equal(t, aaaCol.KindDetails, typing.Invalid)

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
