package columns

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/stretchr/testify/assert"
)

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
				DefaultValue: 123,
			},
		},
		{
			name: "default value set but backfilled",
			column: &Column{
				name:         "id",
				DefaultValue: "dusty",
				backfilled:   true,
			},
		},
		{
			name: "default value set and not backfilled",
			column: &Column{
				name:         "id",
				DefaultValue: "dusty",
			},
			expectShouldBackfill: true,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectShouldBackfill, testCase.column.ShouldBackfill(), testCase.name)
	}
}

func TestUnescapeColumnName(t *testing.T) {
	type _testCase struct {
		escapedName           string
		expectedBigQueryName  string
		expectedSnowflakeName string
		expectedOtherName     string
	}

	testCases := []_testCase{
		{
			escapedName:           "foo",
			expectedBigQueryName:  "foo",
			expectedSnowflakeName: "foo",
			expectedOtherName:     "foo",
		},
		{
			escapedName: "`start`",
			// Should only escape BigQuery
			expectedBigQueryName:  "start",
			expectedSnowflakeName: "`start`",
			expectedOtherName:     "`start`",
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expectedBigQueryName, UnescapeColumnName(testCase.escapedName, constants.BigQuery))
		assert.Equal(t, testCase.expectedSnowflakeName, UnescapeColumnName(testCase.escapedName, constants.Snowflake))
		assert.Equal(t, testCase.expectedSnowflakeName, UnescapeColumnName(testCase.escapedName, constants.SnowflakeStages))
		assert.Equal(t, testCase.expectedOtherName, UnescapeColumnName(testCase.escapedName, ""))
	}
}

func TestColumn_Name(t *testing.T) {
	type _testCase struct {
		colName      string
		expectedName string
		// Snowflake
		expectedNameEsc string
		// BigQuery
		expectedNameEscBq string
	}

	testCases := []_testCase{
		{
			colName:           "start",
			expectedName:      "start",
			expectedNameEsc:   `"start"`, // since this is a reserved word.
			expectedNameEscBq: "`start`", // BQ escapes via backticks.
		},
		{
			colName:           "foo",
			expectedName:      "foo",
			expectedNameEsc:   "foo",
			expectedNameEscBq: "foo",
		},
		{
			colName:           "bar",
			expectedName:      "bar",
			expectedNameEsc:   "bar",
			expectedNameEscBq: "bar",
		},
	}

	for _, testCase := range testCases {
		c := &Column{
			name: testCase.colName,
		}

		assert.Equal(t, testCase.expectedName, c.Name(nil), testCase.colName)
		assert.Equal(t, testCase.expectedName, c.Name(&NameArgs{
			Escape: false,
		}), testCase.colName)

		assert.Equal(t, testCase.expectedNameEsc, c.Name(&NameArgs{
			Escape:   true,
			DestKind: constants.Snowflake,
		}), testCase.colName)
		assert.Equal(t, testCase.expectedNameEscBq, c.Name(&NameArgs{
			Escape:   true,
			DestKind: constants.BigQuery,
		}), testCase.colName)
	}
}

func TestColumns_GetColumnsToUpdate(t *testing.T) {
	type _testCase struct {
		name              string
		cols              []Column
		expectedCols      []string
		expectedColsEsc   []string
		expectedColsEscBq []string
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
			name:              "happy path",
			cols:              happyPathCols,
			expectedCols:      []string{"hi", "bye", "start"},
			expectedColsEsc:   []string{"hi", "bye", `"start"`},
			expectedColsEscBq: []string{"hi", "bye", "`start`"},
		},
		{
			name:              "happy path + extra col",
			cols:              extraCols,
			expectedCols:      []string{"hi", "bye", "start"},
			expectedColsEsc:   []string{"hi", "bye", `"start"`},
			expectedColsEscBq: []string{"hi", "bye", "`start`"},
		},
	}

	for _, testCase := range testCases {
		cols := &Columns{
			columns: testCase.cols,
		}

		assert.Equal(t, testCase.expectedCols, cols.GetColumnsToUpdate(nil), testCase.name)
		assert.Equal(t, testCase.expectedCols, cols.GetColumnsToUpdate(&NameArgs{
			Escape: false,
		}), testCase.name)

		assert.Equal(t, testCase.expectedColsEsc, cols.GetColumnsToUpdate(&NameArgs{
			Escape:   true,
			DestKind: constants.Snowflake,
		}), testCase.name)

		assert.Equal(t, testCase.expectedColsEscBq, cols.GetColumnsToUpdate(&NameArgs{
			Escape:   true,
			DestKind: constants.BigQuery,
		}), testCase.name)
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
			ToastCol: true,
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
		ToastCol:   true,
		PrimaryKey: true,
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
	colsToAdd := []Column{{name: "foo", KindDetails: typing.String}, {name: "bar", KindDetails: typing.Struct}}
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
		name:        "bar",
		KindDetails: typing.Boolean,
	})

	fooCol, isOk = cols.GetColumn("foo")
	assert.True(t, isOk)
	assert.Equal(t, typing.Integer, fooCol.KindDetails)

	barCol, isOk = cols.GetColumn("bar")
	assert.True(t, isOk)
	assert.Equal(t, typing.Boolean, barCol.KindDetails)

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
		happyPathCols       Columns
		stringAndToastCols  Columns
		lastCaseColTypes    Columns
		lastCaseEscapeTypes Columns
	)
	for _, col := range fooBarCols {
		happyPathCols.AddColumn(Column{
			name:        col,
			KindDetails: typing.String,
			ToastColumn: false,
		})
	}
	for _, col := range fooBarCols {
		var toastCol bool
		if col == "foo" {
			toastCol = true
		}

		stringAndToastCols.AddColumn(Column{
			name:        col,
			KindDetails: typing.String,
			ToastColumn: toastCol,
		})
	}

	lastCaseCols := []string{"a1", "b2", "c3"}
	for _, lastCaseCol := range lastCaseCols {
		kd := typing.String
		var toast bool
		// a1 - struct + toast, b2 - string + toast, c3 = regular string.
		if lastCaseCol == "a1" {
			kd = typing.Struct
			toast = true
		} else if lastCaseCol == "b2" {
			toast = true
		}

		lastCaseColTypes.AddColumn(Column{
			name:        lastCaseCol,
			KindDetails: kd,
			ToastColumn: toast,
		})
	}

	lastCaseColsEsc := []string{"a1", "b2", "c3", "`start`", "`select`"}
	for _, lastCaseColEsc := range lastCaseColsEsc {
		kd := typing.String
		var toast bool
		// a1 - struct + toast, b2 - string + toast, c3 = regular string.
		if lastCaseColEsc == "a1" {
			kd = typing.Struct
			toast = true
		} else if lastCaseColEsc == "b2" {
			toast = true
		} else if lastCaseColEsc == "`start`" {
			kd = typing.Struct
			toast = true
		}

		name := lastCaseColEsc
		if name == "`select`" {
			// Unescape (to test that this function escapes it).
			name = "select"
		}

		lastCaseEscapeTypes.AddColumn(Column{
			name:        name,
			KindDetails: kd,
			ToastColumn: toast,
		})
	}

	key := `{"key":"__debezium_unavailable_value"}`

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
			expectedString: `a1= CASE WHEN TO_JSON_STRING(cc.a1) != '{"key":"__debezium_unavailable_value"}' THEN cc.a1 ELSE c.a1 END,b2= CASE WHEN cc.b2 != '__debezium_unavailable_value' THEN cc.b2 ELSE c.b2 END,c3=cc.c3`,
		},
		{
			name:           "struct, string and toast string (bigquery) w/ reserved keywords",
			columns:        lastCaseColsEsc,
			columnsToTypes: lastCaseEscapeTypes,
			bigQuery:       true,
			expectedString: fmt.Sprintf(`a1= CASE WHEN TO_JSON_STRING(cc.a1) != '%s' THEN cc.a1 ELSE c.a1 END,b2= CASE WHEN cc.b2 != '__debezium_unavailable_value' THEN cc.b2 ELSE c.b2 END,c3=cc.c3,%s,%s`,
				key, fmt.Sprintf("`start`= CASE WHEN TO_JSON_STRING(cc.`start`) != '%s' THEN cc.`start` ELSE c.`start` END", key), "`select`=cc.`select`"),
		},
	}

	for _, _testCase := range testCases {
		actualQuery := ColumnsUpdateQuery(_testCase.columns, _testCase.columnsToTypes, _testCase.bigQuery)
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}
}
