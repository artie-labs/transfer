package typing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumn_EscapeName(t *testing.T) {
	type _testCase struct {
		colName      string
		expectedName string
	}

	testCases := []_testCase{
		{
			colName:      "start",
			expectedName: `"start"`, // since this is a reserved word.
		},
		{
			colName:      "foo",
			expectedName: "foo",
		},
		{
			colName:      "bar",
			expectedName: "bar",
		},
	}

	for _, testCase := range testCases {
		c := &Column{
			Name: testCase.colName,
		}

		assert.Equal(t, testCase.expectedName, c.EscapeName(), testCase.colName)
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
				Name:        "hi",
				KindDetails: String,
			},
			{
				Name:        "bye",
				KindDetails: String,
			},
		}
	)

	extraCols := happyPathCols
	for i := 0; i < 100; i++ {
		extraCols = append(extraCols, Column{
			Name:        fmt.Sprintf("hello_%v", i),
			KindDetails: Invalid,
		})
	}

	testCases := []_testCase{
		{
			name:         "happy path",
			cols:         happyPathCols,
			expectedCols: []string{"hi", "bye"},
		},
		{
			name:         "happy path + extra col",
			cols:         extraCols,
			expectedCols: []string{"hi", "bye"},
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
			Name:        key,
			KindDetails: String,
		})
	}

	// Now inspect prior to change.
	for _, col := range cols.GetColumns() {
		assert.False(t, col.ToastColumn)
	}

	// Now selectively update only a, b
	for _, key := range []string{"a", "b"} {
		cols.UpsertColumn(key, true)

		// Now inspect.
		col, _ := cols.GetColumn(key)
		assert.True(t, col.ToastColumn)
	}

	cols.UpsertColumn("zzz", false)
	zzzCol, _ := cols.GetColumn("zzz")
	assert.False(t, zzzCol.ToastColumn)
	assert.Equal(t, zzzCol.KindDetails, Invalid)

	cols.UpsertColumn("aaa", false)
	aaaCol, _ := cols.GetColumn("aaa")
	assert.False(t, aaaCol.ToastColumn)
	assert.Equal(t, aaaCol.KindDetails, Invalid)

	length := len(cols.columns)
	for i := 0; i < 500; i++ {
		cols.UpsertColumn("", false)
	}

	assert.Equal(t, length, len(cols.columns))
}

func TestColumns_Add_Duplicate(t *testing.T) {
	var cols Columns
	duplicateColumns := []Column{{Name: "foo"}, {Name: "foo"}, {Name: "foo"}, {Name: "foo"}, {Name: "foo"}, {Name: "foo"}}
	for _, duplicateColumn := range duplicateColumns {
		cols.AddColumn(duplicateColumn)
	}

	assert.Equal(t, len(cols.GetColumns()), 1, "AddColumn() de-duplicates")
}

func TestColumns_Mutation(t *testing.T) {
	var cols Columns
	colsToAdd := []Column{{Name: "foo", KindDetails: String}, {Name: "bar", KindDetails: Struct}}
	// Insert
	for _, colToAdd := range colsToAdd {
		cols.AddColumn(colToAdd)
	}

	assert.Equal(t, len(cols.GetColumns()), 2)
	fooCol, isOk := cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, String, fooCol.KindDetails)

	barCol, isOk := cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, Struct, barCol.KindDetails)

	// Update
	cols.UpdateColumn(Column{
		Name:        "foo",
		KindDetails: Integer,
	})

	cols.UpdateColumn(Column{
		Name:        "bar",
		KindDetails: Boolean,
	})

	fooCol, isOk = cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, Integer, fooCol.KindDetails)

	barCol, isOk = cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, Boolean, barCol.KindDetails)

	// Delete
	cols.DeleteColumn("foo")
	assert.Equal(t, len(cols.GetColumns()), 1)
	cols.DeleteColumn("bar")
	assert.Equal(t, len(cols.GetColumns()), 0)
}

func TestColumnsUpdateQuery(t *testing.T) {
	type testCase struct {
		name           string
		columns        []string
		columnsToTypes Columns
		expectedString string
		bigQuery       bool
	}

	fooBarCols := []string{"foo", "bar"}

	var (
		happyPathCols      Columns
		stringAndToastCols Columns
		lastCaseColTypes   Columns
	)
	for _, col := range fooBarCols {
		happyPathCols.AddColumn(Column{
			Name:        col,
			KindDetails: String,
			ToastColumn: false,
		})
	}
	for _, col := range fooBarCols {
		var toastCol bool
		if col == "foo" {
			toastCol = true
		}

		stringAndToastCols.AddColumn(Column{
			Name:        col,
			KindDetails: String,
			ToastColumn: toastCol,
		})
	}

	lastCaseCols := []string{"a1", "b2", "c3"}

	for _, lastCaseCol := range lastCaseCols {
		kd := String
		var toast bool
		// a1 - struct + toast, b2 - string + toast, c3 = regular string.
		if lastCaseCol == "a1" {
			kd = Struct
			toast = true
		} else if lastCaseCol == "b2" {
			toast = true
		}

		lastCaseColTypes.AddColumn(Column{
			Name:        lastCaseCol,
			KindDetails: kd,
			ToastColumn: toast,
		})
	}

	testCases := []testCase{
		{
			name:           "happy path",
			columns:        fooBarCols,
			columnsToTypes: happyPathCols,
			expectedString: "foo=cc.foo,bar=cc.bar",
		},
		{
			name:           "string and toast",
			columns:        fooBarCols,
			columnsToTypes: stringAndToastCols,
			expectedString: "foo= CASE WHEN cc.foo != '__debezium_unavailable_value' THEN cc.foo ELSE c.foo END,bar=cc.bar",
		},
		{
			name:           "struct, string and toast string",
			columns:        lastCaseCols,
			columnsToTypes: lastCaseColTypes,
			expectedString: "a1= CASE WHEN cc.a1 != {'key': '__debezium_unavailable_value'} THEN cc.a1 ELSE c.a1 END,b2= CASE WHEN cc.b2 != '__debezium_unavailable_value' THEN cc.b2 ELSE c.b2 END,c3=cc.c3",
		},
		{
			name:           "struct, string and toast string (bigquery)",
			columns:        lastCaseCols,
			columnsToTypes: lastCaseColTypes,
			bigQuery:       true,
			expectedString: `a1= CASE WHEN TO_JSON_STRING(cc.a1) != '{"key": "__debezium_unavailable_value"}' THEN cc.a1 ELSE c.a1 END,b2= CASE WHEN cc.b2 != '__debezium_unavailable_value' THEN cc.b2 ELSE c.b2 END,c3=cc.c3`,
		},
	}

	for _, _testCase := range testCases {
		actualQuery := ColumnsUpdateQuery(_testCase.columns, _testCase.columnsToTypes, _testCase.bigQuery)
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}
}
