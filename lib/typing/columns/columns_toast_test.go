package columns

import (
	"testing"

	"github.com/artie-labs/transfer/lib/sql"
	"github.com/stretchr/testify/assert"
)

func TestProcessToastStructCol(t *testing.T) {
	assert.Equal(t, `foo= CASE WHEN COALESCE(cc.foo != JSON_PARSE('{"key":"__debezium_unavailable_value"}'), true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", sql.RedshiftDialect{}))
	assert.Equal(t, `foo= CASE WHEN COALESCE(TO_JSON_STRING(cc.foo) != '{"key":"__debezium_unavailable_value"}', true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", sql.BigQueryDialect{}))
	assert.Equal(t, `foo= CASE WHEN COALESCE(cc.foo != {'key': '__debezium_unavailable_value'}, true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", sql.SnowflakeDialect{}))
	assert.Equal(t, `foo= CASE WHEN COALESCE(cc.foo, {}) != {'key': '__debezium_unavailable_value'} THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", sql.MSSQLDialect{}))
}

func TestProcessToastCol(t *testing.T) {
	assert.Equal(t, `bar= CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", sql.RedshiftDialect{}))
	assert.Equal(t, `bar= CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", sql.BigQueryDialect{}))
	assert.Equal(t, `bar= CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", sql.SnowflakeDialect{}))
	assert.Equal(t, `bar= CASE WHEN COALESCE(cc.bar, '') != '__debezium_unavailable_value' THEN cc.bar ELSE c.bar END`, processToastCol("bar", sql.MSSQLDialect{}))
}
