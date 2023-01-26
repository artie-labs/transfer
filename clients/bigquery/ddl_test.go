package bigquery

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseSchemaQuery(t *testing.T) {
	basicQueries := []string{
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING) OPTIONS(expiration_timestamp=TIMESTAMP);",
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING)OPTIONS(expiration_timestamp=TIMESTAMP);", // No spacing
		"CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING);",                                        // No OPTIONS
	}

	for _, basicQuery := range basicQueries {
		tableConfig, err := ParseSchemaQuery(map[string]interface{}{
			"ddl": basicQuery,
		})

		assert.NoError(t, err, err)

		assert.Equal(t, len(tableConfig.Columns), 2, tableConfig.Columns)
		for col, kind := range tableConfig.Columns {
			assert.Equal(t, kind, typing.String, fmt.Sprintf("col: %s, kind: %v incorrect", col, kind))
		}
	}
}
