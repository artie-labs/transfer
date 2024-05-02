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

func TestSnowflakeDialect_legacyNeedsEscaping(t *testing.T) {
	dialect := SnowflakeDialect{}
	assert.True(t, dialect.legacyNeedsEscaping("select"))          // name that is reserved
	assert.False(t, dialect.legacyNeedsEscaping("foo"))            // name that is not reserved
	assert.False(t, dialect.legacyNeedsEscaping("__artie_foo"))    // Artie prefix
	assert.True(t, dialect.legacyNeedsEscaping("__artie_foo:bar")) // Artie prefix + symbol
}

func TestSnowflakeDialect_QuoteIdentifier(t *testing.T) {
	{
		// New mode:
		dialect := SnowflakeDialect{LegacyMode: false}
		assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("foo"))
		assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
	}
	{
		// Legacy mode:
		dialect := SnowflakeDialect{LegacyMode: true}
		assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("foo"))
		assert.Equal(t, `"FOO"`, dialect.QuoteIdentifier("FOO"))
		assert.Equal(t, `"abc:def"`, dialect.QuoteIdentifier("abc:def")) // Symbol
		assert.Equal(t, `"select"`, dialect.QuoteIdentifier("select"))   // Reserved name
		assert.Equal(t, `"order"`, dialect.QuoteIdentifier("order"))     // Reserved name
		assert.Equal(t, `"group"`, dialect.QuoteIdentifier("group"))     // Reserved name
		assert.Equal(t, `"start"`, dialect.QuoteIdentifier("start"))     // Reserved name
	}
}
