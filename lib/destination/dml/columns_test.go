package dml

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestQuoteColumns(t *testing.T) {
	assert.Equal(t, []string{}, quoteColumns(nil, sql.BigQueryDialect{}))
	assert.Equal(t, []string{}, quoteColumns(nil, sql.SnowflakeDialect{}))

	cols := []columns.Column{columns.NewColumn("a", typing.Invalid), columns.NewColumn("b", typing.Invalid)}
	assert.Equal(t, []string{"`a`", "`b`"}, quoteColumns(cols, sql.BigQueryDialect{}))
	assert.Equal(t, []string{`"A"`, `"B"`}, quoteColumns(cols, sql.SnowflakeDialect{}))
}

func TestBuildColumnsUpdateFragment(t *testing.T) {
	type testCase struct {
		name           string
		columns        []columns.Column
		expectedString string
		dialect        sql.Dialect
	}

	fooBarCols := []string{"foo", "bar"}

	var (
		happyPathCols       []columns.Column
		stringAndToastCols  []columns.Column
		lastCaseColTypes    []columns.Column
		lastCaseEscapeTypes []columns.Column
	)
	for _, col := range fooBarCols {
		column := columns.NewColumn(col, typing.String)
		column.ToastColumn = false
		happyPathCols = append(happyPathCols, column)
	}
	for _, col := range fooBarCols {
		var toastCol bool
		if col == "foo" {
			toastCol = true
		}

		column := columns.NewColumn(col, typing.String)
		column.ToastColumn = toastCol
		stringAndToastCols = append(stringAndToastCols, column)
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

		column := columns.NewColumn(lastCaseCol, kd)
		column.ToastColumn = toast
		lastCaseColTypes = append(lastCaseColTypes, column)
	}

	lastCaseColsEsc := []string{"a1", "b2", "c3", "start", "select"}
	for _, lastCaseColEsc := range lastCaseColsEsc {
		kd := typing.String
		var toast bool
		// a1 - struct + toast, b2 - string + toast, c3 = regular string.
		if lastCaseColEsc == "a1" {
			kd = typing.Struct
			toast = true
		} else if lastCaseColEsc == "b2" {
			toast = true
		} else if lastCaseColEsc == "start" {
			kd = typing.Struct
			toast = true
		}

		column := columns.NewColumn(lastCaseColEsc, kd)
		column.ToastColumn = toast
		lastCaseEscapeTypes = append(lastCaseEscapeTypes, column)
	}

	lastCaseEscapeTypes = append(lastCaseEscapeTypes, columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	key := `{"key":"__debezium_unavailable_value"}`
	testCases := []testCase{
		{
			name:           "happy path",
			columns:        happyPathCols,
			dialect:        sql.RedshiftDialect{},
			expectedString: `"foo"=cc."foo","bar"=cc."bar"`,
		},
		{
			name:           "string and toast",
			columns:        stringAndToastCols,
			dialect:        sql.SnowflakeDialect{},
			expectedString: `"FOO"= CASE WHEN COALESCE(cc."FOO" != '__debezium_unavailable_value', true) THEN cc."FOO" ELSE c."FOO" END,"BAR"=cc."BAR"`,
		},
		{
			name:           "struct, string and toast string",
			columns:        lastCaseColTypes,
			dialect:        sql.RedshiftDialect{},
			expectedString: `"a1"= CASE WHEN COALESCE(cc."a1" != JSON_PARSE('{"key":"__debezium_unavailable_value"}'), true) THEN cc."a1" ELSE c."a1" END,"b2"= CASE WHEN COALESCE(cc."b2" != '__debezium_unavailable_value', true) THEN cc."b2" ELSE c."b2" END,"c3"=cc."c3"`,
		},
		{
			name:           "struct, string and toast string (bigquery)",
			columns:        lastCaseColTypes,
			dialect:        sql.BigQueryDialect{},
			expectedString: "`a1`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`a1`) != '{\"key\":\"__debezium_unavailable_value\"}', true) THEN cc.`a1` ELSE c.`a1` END,`b2`= CASE WHEN COALESCE(cc.`b2` != '__debezium_unavailable_value', true) THEN cc.`b2` ELSE c.`b2` END,`c3`=cc.`c3`",
		},
		{
			name:    "struct, string and toast string (bigquery) w/ reserved keywords",
			columns: lastCaseEscapeTypes,
			dialect: sql.BigQueryDialect{},
			expectedString: fmt.Sprintf("`a1`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`a1`) != '%s', true) THEN cc.`a1` ELSE c.`a1` END,`b2`= CASE WHEN COALESCE(cc.`b2` != '__debezium_unavailable_value', true) THEN cc.`b2` ELSE c.`b2` END,`c3`=cc.`c3`,%s,%s",
				key, fmt.Sprintf("`start`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`start`) != '%s', true) THEN cc.`start` ELSE c.`start` END", key), "`select`=cc.`select`,`__artie_delete`=cc.`__artie_delete`"),
		},
	}

	for _, _testCase := range testCases {
		actualQuery := buildColumnsUpdateFragment(_testCase.columns, _testCase.dialect)
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}
}

func TestProcessToastStructCol(t *testing.T) {
	assert.Equal(t, `CASE WHEN COALESCE(cc.foo != JSON_PARSE('{"key":"__debezium_unavailable_value"}'), true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", sql.RedshiftDialect{}))
	assert.Equal(t, `CASE WHEN COALESCE(TO_JSON_STRING(cc.foo) != '{"key":"__debezium_unavailable_value"}', true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", sql.BigQueryDialect{}))
	assert.Equal(t, `CASE WHEN COALESCE(cc.foo != {'key': '__debezium_unavailable_value'}, true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", sql.SnowflakeDialect{}))
	assert.Equal(t, `CASE WHEN COALESCE(cc.foo, {}) != {'key': '__debezium_unavailable_value'} THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", sql.MSSQLDialect{}))
}

func TestProcessToastCol(t *testing.T) {
	assert.Equal(t, `CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", sql.RedshiftDialect{}))
	assert.Equal(t, `CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", sql.BigQueryDialect{}))
	assert.Equal(t, `CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", sql.SnowflakeDialect{}))
	assert.Equal(t, `CASE WHEN COALESCE(cc.bar, '') != '__debezium_unavailable_value' THEN cc.bar ELSE c.bar END`, processToastCol("bar", sql.MSSQLDialect{}))
}
