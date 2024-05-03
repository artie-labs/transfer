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

func TestBuildColumnsUpdateFragment(t *testing.T) {
	type testCase struct {
		name           string
		columns        columns.Columns
		expectedString string
		dialect        sql.Dialect
		skipDeleteCol  bool
	}

	fooBarCols := []string{"foo", "bar"}

	var (
		happyPathCols       columns.Columns
		stringAndToastCols  columns.Columns
		lastCaseColTypes    columns.Columns
		lastCaseEscapeTypes columns.Columns
	)
	for _, col := range fooBarCols {
		column := columns.NewColumn(col, typing.String)
		column.ToastColumn = false
		happyPathCols.AddColumn(column)
	}
	for _, col := range fooBarCols {
		var toastCol bool
		if col == "foo" {
			toastCol = true
		}

		column := columns.NewColumn(col, typing.String)
		column.ToastColumn = toastCol
		stringAndToastCols.AddColumn(column)
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
		lastCaseColTypes.AddColumn(column)
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
		lastCaseEscapeTypes.AddColumn(column)
	}

	lastCaseEscapeTypes.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

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
				key, fmt.Sprintf("`start`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`start`) != '%s', true) THEN cc.`start` ELSE c.`start` END", key), "`select`=cc.`select`"),
			skipDeleteCol: true,
		},
		{
			name:    "struct, string and toast string (bigquery) w/ reserved keywords",
			columns: lastCaseEscapeTypes,
			dialect: sql.BigQueryDialect{},
			expectedString: fmt.Sprintf("`a1`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`a1`) != '%s', true) THEN cc.`a1` ELSE c.`a1` END,`b2`= CASE WHEN COALESCE(cc.`b2` != '__debezium_unavailable_value', true) THEN cc.`b2` ELSE c.`b2` END,`c3`=cc.`c3`,%s,%s",
				key, fmt.Sprintf("`start`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`start`) != '%s', true) THEN cc.`start` ELSE c.`start` END", key), "`select`=cc.`select`,`__artie_delete`=cc.`__artie_delete`"),
			skipDeleteCol: false,
		},
	}

	for _, _testCase := range testCases {
		actualQuery := buildColumnsUpdateFragment(&_testCase.columns, _testCase.dialect, _testCase.skipDeleteCol)
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}
}
