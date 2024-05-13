package dml

import (
	"testing"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func TestQuoteColumns(t *testing.T) {
	assert.Equal(t, []string{}, columns.QuoteColumns(nil, bigQueryDialect.BigQueryDialect{}))
	assert.Equal(t, []string{}, columns.QuoteColumns(nil, snowflakeDialect.SnowflakeDialect{}))

	cols := []columns.Column{columns.NewColumn("a", typing.Invalid), columns.NewColumn("b", typing.Invalid)}
	assert.Equal(t, []string{"`a`", "`b`"}, columns.QuoteColumns(cols, bigQueryDialect.BigQueryDialect{}))
	assert.Equal(t, []string{`"A"`, `"B"`}, columns.QuoteColumns(cols, snowflakeDialect.SnowflakeDialect{}))
}
