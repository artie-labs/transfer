package dialect

import (
	"fmt"
	"testing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/stretchr/testify/assert"
)

func TestDatabricksDialect_QuoteIdentifier(t *testing.T) {
	dialect := DatabricksDialect{}
	assert.Equal(t, "`foo`", dialect.QuoteIdentifier("foo"))
	assert.Equal(t, "`FOO`", dialect.QuoteIdentifier("FOO"))
}

func TestDatabricksDialect_IsColumnAlreadyExistsErr(t *testing.T) {
	{
		// No error
		assert.False(t, DatabricksDialect{}.IsColumnAlreadyExistsErr(nil))
	}
	{
		// Random error
		assert.False(t, DatabricksDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf("random error")))
	}
	{
		// Valid
		assert.True(t, DatabricksDialect{}.IsColumnAlreadyExistsErr(fmt.Errorf("[FIELDS_ALREADY_EXISTS] Cannot add column, because `first_name` already exists]")))
	}
}

func TestDatabricksDialect_IsTableDoesNotExistErr(t *testing.T) {
	{
		// No error
		assert.False(t, DatabricksDialect{}.IsTableDoesNotExistErr(nil))
	}
	{
		// Random error
		assert.False(t, DatabricksDialect{}.IsTableDoesNotExistErr(fmt.Errorf("random error")))
	}
	{
		// Valid
		assert.True(t, DatabricksDialect{}.IsTableDoesNotExistErr(fmt.Errorf("[TABLE_OR_VIEW_NOT_FOUND] Table or view not found: `foo`]")))
	}
}

func TestDatabricksDialect_BuildCreateTableQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	{
		// Temporary
		assert.Equal(t,
			`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1}, {PART_2})`,
			DatabricksDialect{}.BuildCreateTableQuery(fakeTableID, true, []string{"{PART_1}", "{PART_2}"}),
		)
	}
	{
		// Not temporary
		assert.Equal(t,
			`CREATE TABLE IF NOT EXISTS {TABLE} ({PART_1}, {PART_2})`,
			DatabricksDialect{}.BuildCreateTableQuery(fakeTableID, false, []string{"{PART_1}", "{PART_2}"}),
		)
	}
}

func TestDatabricksDialect_BuildAlterColumnQuery(t *testing.T) {
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TABLE}")

	{
		// DROP
		assert.Equal(t, "ALTER TABLE {TABLE} drop COLUMN {SQL_PART}", DatabricksDialect{}.BuildAlterColumnQuery(fakeTableID, constants.Delete, "{SQL_PART}"))
	}
	{
		// Add
		assert.Equal(t, "ALTER TABLE {TABLE} add COLUMN {SQL_PART} {DATA_TYPE}", DatabricksDialect{}.BuildAlterColumnQuery(fakeTableID, constants.Add, "{SQL_PART} {DATA_TYPE}"))
	}
}

func TestDatabricksDialect_BuildDedupeQueries(t *testing.T) {
	dialect := DatabricksDialect{}
	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns("{TARGET}")

	fakeStagingTableID := &mocks.FakeTableIdentifier{}
	fakeStagingTableID.FullyQualifiedNameReturns("{STAGING}")

	{
		// includeArtieUpdatedAt = true
		queries := dialect.BuildDedupeQueries(fakeTableID, fakeStagingTableID, []string{"id"}, true)
		assert.Len(t, queries, 3)
		assert.Equal(t,
			fmt.Sprintf("CREATE TABLE {STAGING} AS SELECT * FROM {TARGET} QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s ASC, %s ASC) = 2",
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier(constants.UpdateColumnMarker),
			),
			queries[0])
		assert.Equal(t,
			fmt.Sprintf("DELETE FROM {TARGET} t1 WHERE EXISTS (SELECT * FROM {STAGING} t2 WHERE t1.%s = t2.%s)",
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier("id"),
			),
			queries[1])
		assert.Equal(t, "INSERT INTO {TARGET} SELECT * FROM {STAGING}", queries[2])
	}
	{
		// includeArtieUpdatedAt = false
		queries := dialect.BuildDedupeQueries(fakeTableID, fakeStagingTableID, []string{"id"}, false)
		assert.Len(t, queries, 3)
		assert.Equal(t,
			fmt.Sprintf("CREATE TABLE {STAGING} AS SELECT * FROM {TARGET} QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s ASC) = 2",
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier("id")),
			queries[0])
		assert.Equal(t,
			fmt.Sprintf("DELETE FROM {TARGET} t1 WHERE EXISTS (SELECT * FROM {STAGING} t2 WHERE t1.%s = t2.%s)",
				dialect.QuoteIdentifier("id"),
				dialect.QuoteIdentifier("id")),
			queries[1])
		assert.Equal(t, "INSERT INTO {TARGET} SELECT * FROM {STAGING}", queries[2])
	}

}
