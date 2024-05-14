package tests

import (
	"fmt"
	"testing"

	bigqueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	redshiftDialect "github.com/artie-labs/transfer/clients/redshift/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestQuoteColumns(t *testing.T) {
	{
		// BigQuery:
		assert.Equal(t, []string{}, sql.QuoteColumns(nil, bigqueryDialect.BigQueryDialect{}))
		cols := []columns.Column{columns.NewColumn("a", typing.Invalid), columns.NewColumn("b", typing.Invalid)}
		assert.Equal(t, []string{"`a`", "`b`"}, sql.QuoteColumns(cols, bigqueryDialect.BigQueryDialect{}))
	}
	{
		// Snowflake:
		assert.Equal(t, []string{}, sql.QuoteColumns(nil, snowflakeDialect.SnowflakeDialect{}))
		cols := []columns.Column{columns.NewColumn("a", typing.Invalid), columns.NewColumn("b", typing.Invalid)}
		assert.Equal(t, []string{`"A"`, `"B"`}, sql.QuoteColumns(cols, snowflakeDialect.SnowflakeDialect{}))
	}
}

func TestBuildColumnsUpdateFragment_BigQuery(t *testing.T) {
	var lastCaseColTypes []columns.Column
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

	var lastCaseEscapeTypes []columns.Column
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
	testCases := []struct {
		name           string
		columns        []columns.Column
		expectedString string
	}{
		{
			name:           "struct, string and toast string (bigquery)",
			columns:        lastCaseColTypes,
			expectedString: "`a1`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`a1`) != '{\"key\":\"__debezium_unavailable_value\"}', true) THEN cc.`a1` ELSE c.`a1` END,`b2`= CASE WHEN COALESCE(cc.`b2` != '__debezium_unavailable_value', true) THEN cc.`b2` ELSE c.`b2` END,`c3`=cc.`c3`",
		},
		{
			name:    "struct, string and toast string (bigquery) w/ reserved keywords",
			columns: lastCaseEscapeTypes,
			expectedString: fmt.Sprintf("`a1`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`a1`) != '%s', true) THEN cc.`a1` ELSE c.`a1` END,`b2`= CASE WHEN COALESCE(cc.`b2` != '__debezium_unavailable_value', true) THEN cc.`b2` ELSE c.`b2` END,`c3`=cc.`c3`,%s,%s",
				key, fmt.Sprintf("`start`= CASE WHEN COALESCE(TO_JSON_STRING(cc.`start`) != '%s', true) THEN cc.`start` ELSE c.`start` END", key), "`select`=cc.`select`,`__artie_delete`=cc.`__artie_delete`"),
		},
	}

	for _, _testCase := range testCases {
		actualQuery := sql.BuildColumnsUpdateFragment(_testCase.columns, "cc", "c", bigqueryDialect.BigQueryDialect{})
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}
}

func TestBuildColumnsUpdateFragment_Redshift(t *testing.T) {
	var happyPathCols []columns.Column
	for _, col := range []string{"foo", "bar"} {
		column := columns.NewColumn(col, typing.String)
		column.ToastColumn = false
		happyPathCols = append(happyPathCols, column)
	}

	var lastCaseColTypes []columns.Column
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

	testCases := []struct {
		name           string
		columns        []columns.Column
		expectedString string
	}{
		{
			name:           "happy path",
			columns:        happyPathCols,
			expectedString: `"foo"=cc."foo","bar"=cc."bar"`,
		},
		{
			name:           "struct, string and toast string",
			columns:        lastCaseColTypes,
			expectedString: `"a1"= CASE WHEN COALESCE(cc."a1" != JSON_PARSE('{"key":"__debezium_unavailable_value"}'), true) THEN cc."a1" ELSE c."a1" END,"b2"= CASE WHEN COALESCE(cc."b2" != '__debezium_unavailable_value', true) THEN cc."b2" ELSE c."b2" END,"c3"=cc."c3"`,
		},
	}

	for _, _testCase := range testCases {
		actualQuery := sql.BuildColumnsUpdateFragment(_testCase.columns, "cc", "c", redshiftDialect.RedshiftDialect{})
		assert.Equal(t, _testCase.expectedString, actualQuery, _testCase.name)
	}
}

func TestBuildColumnsUpdateFragment_Snowflake(t *testing.T) {
	var stringAndToastCols []columns.Column
	for _, col := range []string{"foo", "bar"} {
		var toastCol bool
		if col == "foo" {
			toastCol = true
		}

		column := columns.NewColumn(col, typing.String)
		column.ToastColumn = toastCol
		stringAndToastCols = append(stringAndToastCols, column)
	}

	actualQuery := sql.BuildColumnsUpdateFragment(stringAndToastCols, "cc", "c", snowflakeDialect.SnowflakeDialect{})
	assert.Equal(t, `"FOO"= CASE WHEN COALESCE(cc."FOO" != '__debezium_unavailable_value', true) THEN cc."FOO" ELSE c."FOO" END,"BAR"=cc."BAR"`, actualQuery)
}

func TestBuildColumnComparison(t *testing.T) {
	col := columns.NewColumn("foo", typing.Boolean)
	dialect := snowflakeDialect.SnowflakeDialect{}
	assert.Equal(t, `a."FOO" = b."FOO"`, sql.BuildColumnComparison(col, "a", "b", sql.Equal, dialect))
	assert.Equal(t, `a."FOO" >= b."FOO"`, sql.BuildColumnComparison(col, "a", "b", sql.GreaterThanOrEqual, dialect))
}

func TestBuildColumnComparisons(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("foo", typing.Boolean),
		columns.NewColumn("bar", typing.String),
	}
	dialect := snowflakeDialect.SnowflakeDialect{}
	assert.Equal(t, []string{`a."FOO" = b."FOO"`, `a."BAR" = b."BAR"`}, sql.BuildColumnComparisons(cols, "a", "b", sql.Equal, dialect))
}
