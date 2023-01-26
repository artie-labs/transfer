package bigquery

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseSchemaQuery(t *testing.T) {
	rows := map[string]interface{}{
		"ddl": "CREATE TABLE `artie-labs.mock.customers`(string_field_0 STRING,string_field_1 STRING)OPTIONS(expiration_timestamp=TIMESTAMP \"2023-03-26T20:03:44.504Z\");",
	}

	tableConfig, err := ParseSchemaQuery(rows)
	fmt.Print("tableConfig", tableConfig, "err", err)
	assert.False(t, true)
}
