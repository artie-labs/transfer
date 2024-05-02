package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMSSQLDialect_QuoteIdentifier(t *testing.T) {
	dialect := MSSQLDialect{}
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
}

func TestBigQueryDialect_QuoteIdentifier(t *testing.T) {
	dialect := BigQueryDialect{}
	assert.Equal(t, "`foo`", dialect.QuoteIdentifier("foo"))
	assert.Equal(t, "`FOO`", dialect.QuoteIdentifier("FOO"))
}

func TestRedshiftDialect_QuoteIdentifier(t *testing.T) {
	dialect := RedshiftDialect{}
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"foo"`, dialect.QuoteIdentifier("FOO"))
}

func TestSnowflakeDialect_NeedsEscaping(t *testing.T) {
	// UppercaseEscNames enabled:
	dialect := SnowflakeDialect{}

	assert.True(t, dialect.NeedsEscaping("select"))          // name that is reserved
	assert.True(t, dialect.NeedsEscaping("foo"))             // name that is not reserved
	assert.True(t, dialect.NeedsEscaping("__artie_foo"))     // Artie prefix
	assert.True(t, dialect.NeedsEscaping("__artie_foo:bar")) // Artie prefix + symbol
}

func TestSnowflakeDialect_QuoteIdentifier(t *testing.T) {
	// UppercaseEscNames enabled:
	dialect := SnowflakeDialect{}
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("foo"))
	assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
}
