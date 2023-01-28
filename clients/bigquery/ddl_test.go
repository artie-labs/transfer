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
		tableConfig, err := ParseSchemaQuery(basicQuery, false)

		assert.NoError(t, err, err)

		assert.Equal(t, len(tableConfig.Columns()), 2, tableConfig.Columns)
		for col, kind := range tableConfig.Columns() {
			assert.Equal(t, kind, typing.String, fmt.Sprintf("col: %s, kind: %v incorrect", col, kind))
		}
	}
}

func TestParseSchemaQueryComplex(t *testing.T) {
	// This test will test every single data type.
	tableConfig, err := ParseSchemaQuery("CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING,field2 INT64,field3 ARRAY<INT64>,field4 FLOAT64,field5 NUMERIC,field6 BIGNUMERIC,field7 BOOL,field8 TIMESTAMP,field9 DATE,field10 TIME,field11 DATETIME,field12 STRUCT<foo STRING>,field13 JSON)OPTIONS(expiration_timestamp=TIMESTAMP 2023-03-26T20:03:44.504Z);",
		false)

	assert.NoError(t, err, err)
	assert.Equal(t, len(tableConfig.Columns()), 14, tableConfig.Columns)

	// TODO test and iterate over every column

}
