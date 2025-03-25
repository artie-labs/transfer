package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestAddPrefixToTableName(t *testing.T) {
	const prefix = "%"
	{
		// Database, schema and table name
		assert.Equal(t, `"DATABASE"."SCHEMA"."%TABLENAME"`, addPrefixToTableName(dialect.NewTableIdentifier("database", "schema", "tableName"), prefix))
	}
	{
		// Table name
		assert.Equal(t, `"".""."%ORDERS"`, addPrefixToTableName(dialect.NewTableIdentifier("", "", "orders"), prefix))
	}
	{
		// Schema and table name
		assert.Equal(t, `""."PUBLIC"."%ORDERS"`, addPrefixToTableName(dialect.NewTableIdentifier("", "public", "orders"), prefix))
	}
	{
		// Database and table name
		assert.Equal(t, `"DB".""."%TABLENAME"`, addPrefixToTableName(dialect.NewTableIdentifier("db", "", "tableName"), prefix))
	}
}

func (s *SnowflakeTestSuite) TestEscapeColumns() {
	{
		// Test basic string columns
		var cols columns.Columns
		cols.AddColumn(columns.NewColumn("foo", typing.String))
		cols.AddColumn(columns.NewColumn("bar", typing.String))
		assert.Equal(s.T(), "$1,$2", escapeColumns(&cols, ","))
	}
	{
		// Test string columns with struct
		var cols columns.Columns
		cols.AddColumn(columns.NewColumn("foo", typing.String))
		cols.AddColumn(columns.NewColumn("bar", typing.String))
		cols.AddColumn(columns.NewColumn("struct", typing.Struct))
		assert.Equal(s.T(), "$1,$2,PARSE_JSON($3)", escapeColumns(&cols, ","))
	}
	{
		// Test string columns with struct and array
		var cols columns.Columns
		cols.AddColumn(columns.NewColumn("foo", typing.String))
		cols.AddColumn(columns.NewColumn("bar", typing.String))
		cols.AddColumn(columns.NewColumn("struct", typing.Struct))
		cols.AddColumn(columns.NewColumn("array", typing.Array))
		assert.Equal(s.T(), "$1,$2,PARSE_JSON($3),CAST(PARSE_JSON($4) AS ARRAY) AS $4", escapeColumns(&cols, ","))
	}
	{
		// Test with invalid columns mixed in
		var cols columns.Columns
		cols.AddColumn(columns.NewColumn("invalid1", typing.Invalid))
		cols.AddColumn(columns.NewColumn("foo", typing.String))
		cols.AddColumn(columns.NewColumn("bar", typing.String))
		cols.AddColumn(columns.NewColumn("struct", typing.Struct))
		cols.AddColumn(columns.NewColumn("array", typing.Array))
		cols.AddColumn(columns.NewColumn("invalid2", typing.Invalid))
		assert.Equal(s.T(), "$1,$2,PARSE_JSON($3),CAST(PARSE_JSON($4) AS ARRAY) AS $4", escapeColumns(&cols, ","))
	}
}
