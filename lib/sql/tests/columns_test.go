package tests

import (
	"testing"

	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

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
