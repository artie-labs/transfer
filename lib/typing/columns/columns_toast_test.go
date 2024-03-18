package columns

import (
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
)

func TestProcessToastStructCol(t *testing.T) {
	assert.Equal(t, `foo= CASE WHEN COALESCE(cc.foo != JSON_PARSE('{"key":"__debezium_unavailable_value"}'), true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", constants.Redshift))
	assert.Equal(t, `foo= CASE WHEN COALESCE(TO_JSON_STRING(cc.foo) != '{"key":"__debezium_unavailable_value"}', true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", constants.BigQuery))
	assert.Equal(t, `foo= CASE WHEN COALESCE(cc.foo != {'key': '__debezium_unavailable_value'}, true) THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", constants.Snowflake))
	assert.Equal(t, `foo= CASE WHEN COALESCE(cc.foo, {}) != {'key': '__debezium_unavailable_value'} THEN cc.foo ELSE c.foo END`, processToastStructCol("foo", constants.MSSQL))
}

func TestProcessToastCol(t *testing.T) {
	assert.Equal(t, `bar= CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", constants.Redshift))
	assert.Equal(t, `bar= CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", constants.BigQuery))
	assert.Equal(t, `bar= CASE WHEN COALESCE(cc.bar != '__debezium_unavailable_value', true) THEN cc.bar ELSE c.bar END`, processToastCol("bar", constants.Snowflake))
	assert.Equal(t, `bar= CASE WHEN COALESCE(cc.bar, '') != '__debezium_unavailable_value' THEN cc.bar ELSE c.bar END`, processToastCol("bar", constants.MSSQL))
}
