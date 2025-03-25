package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
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
