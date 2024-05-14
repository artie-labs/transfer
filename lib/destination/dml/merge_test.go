package dml

import (
	"testing"

	"github.com/stretchr/testify/assert"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
)

func TestMergeArgument_BuildStatements_Validation(t *testing.T) {
	for _, arg := range []*MergeArgument{
		{Dialect: snowflakeDialect.SnowflakeDialect{}},
		{Dialect: bigQueryDialect.BigQueryDialect{}},
	} {
		parts, err := arg.BuildStatements()
		assert.ErrorContains(t, err, "merge argument does not contain primary keys")
		assert.Nil(t, parts)
	}
}
