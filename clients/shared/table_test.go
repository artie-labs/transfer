package shared

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/sql"
)

func TestBuildStagingTableID(t *testing.T) {
	{
		// When pair is invalid (empty database and schema), should use the original tableID
		fakeBaseline := &mocks.FakeBaseline{}
		originalTableID := dialect.NewTableIdentifier("original_db", "original_schema", "my_table")
		pair := kafkalib.DatabaseAndSchemaPair{Database: "", Schema: ""}

		result := BuildStagingTableID(fakeBaseline, pair, originalTableID)

		// IdentifierFor should not be called when pair is invalid
		assert.Equal(t, 0, fakeBaseline.IdentifierForCallCount())
		// Should be a temp table
		assert.True(t, result.TemporaryTable())
		// Should contain the original table name with artie prefix
		assert.True(t, strings.HasPrefix(result.Table(), "my_table___artie_"))
		// Should use the original database and schema
		assert.Contains(t, result.FullyQualifiedName(), `"ORIGINAL_DB"."ORIGINAL_SCHEMA"`)
	}
	{
		// When pair is invalid (empty database only), should use the original tableID
		fakeBaseline := &mocks.FakeBaseline{}
		originalTableID := dialect.NewTableIdentifier("original_db", "original_schema", "my_table")
		pair := kafkalib.DatabaseAndSchemaPair{Database: "", Schema: "staging_schema"}

		result := BuildStagingTableID(fakeBaseline, pair, originalTableID)

		// IdentifierFor should not be called when pair is invalid
		assert.Equal(t, 0, fakeBaseline.IdentifierForCallCount())
		// Should be a temp table
		assert.True(t, result.TemporaryTable())
		// Should contain the original table name with artie prefix
		assert.True(t, strings.HasPrefix(result.Table(), "my_table___artie_"))
		// Should use the original database and schema (not the staging schema)
		assert.Contains(t, result.FullyQualifiedName(), `"ORIGINAL_DB"."ORIGINAL_SCHEMA"`)
	}
	{
		// When pair is invalid (empty schema only), should use the original tableID
		fakeBaseline := &mocks.FakeBaseline{}
		originalTableID := dialect.NewTableIdentifier("original_db", "original_schema", "my_table")
		pair := kafkalib.DatabaseAndSchemaPair{Database: "staging_db", Schema: ""}

		result := BuildStagingTableID(fakeBaseline, pair, originalTableID)

		// IdentifierFor should not be called when pair is invalid
		assert.Equal(t, 0, fakeBaseline.IdentifierForCallCount())
		// Should be a temp table
		assert.True(t, result.TemporaryTable())
		// Should contain the original table name with artie prefix
		assert.True(t, strings.HasPrefix(result.Table(), "my_table___artie_"))
		// Should use the original database and schema (not the staging database)
		assert.Contains(t, result.FullyQualifiedName(), `"ORIGINAL_DB"."ORIGINAL_SCHEMA"`)
	}
	{
		// When pair is valid, should use IdentifierFor to create a new tableID with the staging schema
		fakeBaseline := &mocks.FakeBaseline{}
		fakeBaseline.IdentifierForStub = func(pair kafkalib.DatabaseAndSchemaPair, table string) sql.TableIdentifier {
			return dialect.NewTableIdentifier(pair.Database, pair.Schema, table)
		}

		originalTableID := dialect.NewTableIdentifier("original_db", "original_schema", "my_table")
		pair := kafkalib.DatabaseAndSchemaPair{Database: "staging_db", Schema: "staging_schema"}

		result := BuildStagingTableID(fakeBaseline, pair, originalTableID)

		// IdentifierFor should be called once
		assert.Equal(t, 1, fakeBaseline.IdentifierForCallCount())
		actualPair, actualTable := fakeBaseline.IdentifierForArgsForCall(0)
		assert.Equal(t, pair, actualPair)
		assert.Equal(t, "my_table", actualTable)
		// Should be a temp table
		assert.True(t, result.TemporaryTable())
		// Should contain the original table name with artie prefix
		assert.True(t, strings.HasPrefix(result.Table(), "my_table___artie_"))
		// Should use the staging database and schema from the pair
		assert.Contains(t, result.FullyQualifiedName(), `"STAGING_DB"."STAGING_SCHEMA"`)
	}
}
