package shared

import (
	"testing"

	bq "github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/stretchr/testify/assert"
)

func TestBuildAdditionalEqualityStrings(t *testing.T) {
	{
		// No predicates
		out, err := BuildAdditionalEqualityStrings(nil, []partition.MergePredicates{})
		assert.NoError(t, err)
		assert.Empty(t, out)
	}
	{
		// One merge predicate
		{
			// Snowflake
			var snowflake dialect.SnowflakeDialect
			out, err := BuildAdditionalEqualityStrings(&snowflake, []partition.MergePredicates{{PartitionField: "id"}})
			assert.NoError(t, err)
			assert.Equal(t, []string{`tgt."ID" = stg."ID"`}, out)
		}
		{
			// BigQuery
			var bigquery bq.BigQueryDialect
			out, err := BuildAdditionalEqualityStrings(&bigquery, []partition.MergePredicates{{PartitionField: "id"}})
			assert.NoError(t, err)
			assert.Equal(t, []string{"tgt.`id` = stg.`id`"}, out)
		}
	}
	{
		// Two merge predicates
		{
			// Snowflake
			var snowflake dialect.SnowflakeDialect
			out, err := BuildAdditionalEqualityStrings(&snowflake, []partition.MergePredicates{{PartitionField: "id"}, {PartitionField: "name"}})
			assert.NoError(t, err)
			assert.Equal(t, []string{`tgt."ID" = stg."ID"`, `tgt."NAME" = stg."NAME"`}, out)
		}
		{
			// BigQuery
			var bigquery bq.BigQueryDialect
			out, err := BuildAdditionalEqualityStrings(&bigquery, []partition.MergePredicates{{PartitionField: "id"}, {PartitionField: "name"}})
			assert.NoError(t, err)
			assert.Equal(t, []string{"tgt.`id` = stg.`id`", "tgt.`name` = stg.`name`"}, out)
		}
	}
}
