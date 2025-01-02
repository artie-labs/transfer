package tests

import (
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
	cols := []columns.Column{columns.NewColumn("a", typing.Invalid), columns.NewColumn("b", typing.Invalid)}

	// BigQuery:
	assert.Equal(t, []string{}, sql.QuoteColumns(nil, bigqueryDialect.BigQueryDialect{}))
	assert.Equal(t, []string{"`a`", "`b`"}, sql.QuoteColumns(cols, bigqueryDialect.BigQueryDialect{}))

	// Snowflake:
	assert.Equal(t, []string{}, sql.QuoteColumns(nil, snowflakeDialect.SnowflakeDialect{}))
	assert.Equal(t, []string{`"A"`, `"B"`}, sql.QuoteColumns(cols, snowflakeDialect.SnowflakeDialect{}))
}

func TestQuoteTableAliasColumn(t *testing.T) {
	column := columns.NewColumn("col", typing.Invalid)

	// BigQuery:
	assert.Equal(t, "tbl.`col`", sql.QuoteTableAliasColumn("tbl", column, bigqueryDialect.BigQueryDialect{}))
	// Snowflake:
	assert.Equal(t, `tbl."COL"`, sql.QuoteTableAliasColumn("tbl", column, snowflakeDialect.SnowflakeDialect{}))
}

func TestQuoteTableAliasColumns(t *testing.T) {
	cols := []columns.Column{columns.NewColumn("a", typing.Invalid), columns.NewColumn("b", typing.Invalid)}

	// BigQuery:
	assert.Equal(t, []string{}, sql.QuoteTableAliasColumns("foo", nil, bigqueryDialect.BigQueryDialect{}))
	assert.Equal(t, []string{"foo.`a`", "foo.`b`"}, sql.QuoteTableAliasColumns("foo", cols, bigqueryDialect.BigQueryDialect{}))

	// Snowflake:
	assert.Equal(t, []string{}, sql.QuoteTableAliasColumns("foo", nil, snowflakeDialect.SnowflakeDialect{}))
	assert.Equal(t, []string{`foo."A"`, `foo."B"`}, sql.QuoteTableAliasColumns("foo", cols, snowflakeDialect.SnowflakeDialect{}))
}

func TestQuotedDeleteColumnMarker(t *testing.T) {
	// BigQuery:
	assert.Equal(t, "tbl.`__artie_delete`", sql.QuotedDeleteColumnMarker("tbl", bigqueryDialect.BigQueryDialect{}))
	// Snowflake:
	assert.Equal(t, `tbl."__ARTIE_DELETE"`, sql.QuotedDeleteColumnMarker("tbl", snowflakeDialect.SnowflakeDialect{}))
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

	testCases := []struct {
		name           string
		columns        []columns.Column
		expectedString string
	}{
		{
			name:           "struct, string and toast string (bigquery)",
			columns:        lastCaseColTypes,
			expectedString: "`a1`= CASE WHEN TO_JSON_STRING(stg.`a1`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`a1` ELSE tgt.`a1` END,`b2`= CASE WHEN TO_JSON_STRING(stg.`b2`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`b2` ELSE tgt.`b2` END,`c3`=stg.`c3`",
		},
		{
			name:           "struct, string and toast string (bigquery) w/ reserved keywords",
			columns:        lastCaseEscapeTypes,
			expectedString: "`a1`= CASE WHEN TO_JSON_STRING(stg.`a1`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`a1` ELSE tgt.`a1` END,`b2`= CASE WHEN TO_JSON_STRING(stg.`b2`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`b2` ELSE tgt.`b2` END,`c3`=stg.`c3`,`start`= CASE WHEN TO_JSON_STRING(stg.`start`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`start` ELSE tgt.`start` END,`select`=stg.`select`,`__artie_delete`=stg.`__artie_delete`",
		},
	}

	for _, _testCase := range testCases {
		actualQuery := sql.BuildColumnsUpdateFragment(_testCase.columns, "stg", "tgt", bigqueryDialect.BigQueryDialect{})
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
			expectedString: `"foo"=stg."foo","bar"=stg."bar"`,
		},
		{
			name:    "struct, string and toast string",
			columns: lastCaseColTypes,
			expectedString: `"a1"= CASE WHEN 
COALESCE(
    CASE
        WHEN JSON_SIZE(stg."a1") < 500 THEN JSON_SERIALIZE(stg."a1") NOT LIKE '%__debezium_unavailable_value%'
    ELSE
        TRUE
    END,
    TRUE
) THEN stg."a1" ELSE tgt."a1" END,"b2"= CASE WHEN COALESCE(stg."b2" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."b2" ELSE tgt."b2" END,"c3"=stg."c3"`,
		},
	}

	for _, _testCase := range testCases {
		actualQuery := sql.BuildColumnsUpdateFragment(_testCase.columns, "stg", "tgt", redshiftDialect.RedshiftDialect{})
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

	actualQuery := sql.BuildColumnsUpdateFragment(stringAndToastCols, "stg", "tgt", snowflakeDialect.SnowflakeDialect{})
	assert.Equal(t, `"FOO"= CASE WHEN COALESCE(stg."FOO" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."FOO" ELSE tgt."FOO" END,"BAR"=stg."BAR"`, actualQuery)
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
