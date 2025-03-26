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
	{
		// Test basic mixed columns with struct, toast string, and regular string
		var cols []columns.Column
		// Add struct column with toast
		structCol := columns.NewColumn("a1", typing.Struct)
		structCol.ToastColumn = true
		cols = append(cols, structCol)

		// Add string column with toast
		toastStringCol := columns.NewColumn("b2", typing.String)
		toastStringCol.ToastColumn = true
		cols = append(cols, toastStringCol)

		// Add regular string column
		regularStringCol := columns.NewColumn("c3", typing.String)
		regularStringCol.ToastColumn = false
		cols = append(cols, regularStringCol)

		expectedQuery := "`a1`= CASE WHEN TO_JSON_STRING(stg.`a1`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`a1` ELSE tgt.`a1` END,`b2`= CASE WHEN TO_JSON_STRING(stg.`b2`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`b2` ELSE tgt.`b2` END,`c3`=stg.`c3`"
		assert.Equal(t, expectedQuery, sql.BuildColumnsUpdateFragment(cols, "stg", "tgt", bigqueryDialect.BigQueryDialect{}), "mixed columns with struct, toast string, and regular string")
	}
	{
		// Test mixed columns with reserved keywords and delete marker
		var cols []columns.Column
		// Add struct column with toast
		structCol := columns.NewColumn("a1", typing.Struct)
		structCol.ToastColumn = true
		cols = append(cols, structCol)

		// Add string column with toast
		toastStringCol := columns.NewColumn("b2", typing.String)
		toastStringCol.ToastColumn = true
		cols = append(cols, toastStringCol)

		// Add regular string column
		regularStringCol := columns.NewColumn("c3", typing.String)
		regularStringCol.ToastColumn = false
		cols = append(cols, regularStringCol)

		// Add struct column with reserved keyword
		reservedStructCol := columns.NewColumn("start", typing.Struct)
		reservedStructCol.ToastColumn = true
		cols = append(cols, reservedStructCol)

		// Add regular string column with reserved keyword
		reservedStringCol := columns.NewColumn("select", typing.String)
		reservedStringCol.ToastColumn = false
		cols = append(cols, reservedStringCol)

		// Add delete marker column
		deleteMarkerCol := columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)
		deleteMarkerCol.ToastColumn = false
		cols = append(cols, deleteMarkerCol)

		expectedQuery := "`a1`= CASE WHEN TO_JSON_STRING(stg.`a1`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`a1` ELSE tgt.`a1` END,`b2`= CASE WHEN TO_JSON_STRING(stg.`b2`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`b2` ELSE tgt.`b2` END,`c3`=stg.`c3`,`start`= CASE WHEN TO_JSON_STRING(stg.`start`) NOT LIKE '%__debezium_unavailable_value%' THEN stg.`start` ELSE tgt.`start` END,`select`=stg.`select`,`__artie_delete`=stg.`__artie_delete`"
		assert.Equal(t, expectedQuery, sql.BuildColumnsUpdateFragment(cols, "stg", "tgt", bigqueryDialect.BigQueryDialect{}), "mixed columns with reserved keywords and delete marker")
	}
}

func TestBuildColumnsUpdateFragment_Redshift(t *testing.T) {
	{
		// Test basic string columns without toast
		var cols []columns.Column
		for _, col := range []string{"foo", "bar"} {
			column := columns.NewColumn(col, typing.String)
			column.ToastColumn = false
			cols = append(cols, column)
		}

		actualQuery := sql.BuildColumnsUpdateFragment(cols, "stg", "tgt", redshiftDialect.RedshiftDialect{})
		assert.Equal(t, `"foo"=stg."foo","bar"=stg."bar"`, actualQuery)
	}
	{
		// Test mixed columns with struct, toast string, and regular string
		var cols []columns.Column

		// Add struct column with toast
		structCol := columns.NewColumn("a1", typing.Struct)
		structCol.ToastColumn = true
		cols = append(cols, structCol)

		// Add string column with toast
		toastStringCol := columns.NewColumn("b2", typing.String)
		toastStringCol.ToastColumn = true
		cols = append(cols, toastStringCol)

		// Add regular string column
		regularStringCol := columns.NewColumn("c3", typing.String)
		regularStringCol.ToastColumn = false
		cols = append(cols, regularStringCol)

		actualQuery := sql.BuildColumnsUpdateFragment(cols, "stg", "tgt", redshiftDialect.RedshiftDialect{})
		expectedQuery := `"a1"= CASE WHEN 
COALESCE(
    CASE
        WHEN JSON_SIZE(stg."a1") < 500 THEN JSON_SERIALIZE(stg."a1") NOT LIKE '%__debezium_unavailable_value%'
    ELSE
        TRUE
    END,
    TRUE
) THEN stg."a1" ELSE tgt."a1" END,"b2"= CASE WHEN COALESCE(stg."b2" NOT LIKE '%__debezium_unavailable_value%', TRUE) THEN stg."b2" ELSE tgt."b2" END,"c3"=stg."c3"`
		assert.Equal(t, expectedQuery, actualQuery)
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
