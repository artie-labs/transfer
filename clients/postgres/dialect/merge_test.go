package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestPostgresDialect_buildSoftDeleteMergeQuery(t *testing.T) {
	dialect := PostgresDialect{}
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns(`"database"."schema"."table"`)

	subQuery := "SELECT * FROM staging"
	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
	}
	joinCondition := `tgt."id" = stg."id"`

	query := dialect.buildSoftDeleteMergeQuery(tableID, subQuery, joinCondition, cols)

	expectedQuery := `
MERGE INTO "database"."schema"."table" AS tgt
USING SELECT * FROM staging AS stg ON tgt."id" = stg."id"
WHEN MATCHED AND COALESCE(stg."__artie_only_set_delete", false) = false THEN UPDATE SET "id"=stg."id","name"=stg."name","__artie_delete"=stg."__artie_delete"
WHEN MATCHED AND COALESCE(stg."__artie_only_set_delete", false) = true THEN UPDATE SET "__artie_delete"=stg."__artie_delete"
WHEN NOT MATCHED THEN INSERT ("id","name","__artie_delete") VALUES (stg."id",stg."name",stg."__artie_delete")`

	assert.Equal(t, expectedQuery, query)
}

func TestPostgresDialect_buildRegularMergeQuery(t *testing.T) {
	dialect := PostgresDialect{}
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns(`"database"."schema"."table"`)

	subQuery := "SELECT * FROM staging"
	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
	}
	joinCondition := `tgt."id" = stg."id"`

	query := dialect.buildRegularMergeQuery(tableID, subQuery, joinCondition, cols)

	expectedQuery := `
MERGE INTO "database"."schema"."table" AS tgt USING SELECT * FROM staging AS stg ON tgt."id" = stg."id"
WHEN MATCHED AND stg."__artie_delete" = true THEN DELETE
WHEN MATCHED AND COALESCE(stg."__artie_delete", false) = false THEN UPDATE SET "id"=stg."id","name"=stg."name"
WHEN NOT MATCHED AND COALESCE(stg."__artie_delete", false) = false THEN INSERT ("id","name") VALUES (stg."id",stg."name")`

	assert.Equal(t, expectedQuery, query)
}

func TestPostgresDialect_BuildMergeQueries(t *testing.T) {
	dialect := PostgresDialect{}
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns(`"database"."schema"."table"`)

	subQuery := "SELECT * FROM staging"
	primaryKeys := []columns.Column{
		columns.NewColumn("id", typing.String),
	}

	// Test soft delete mode
	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	queries, err := dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, nil, cols, true, false)
	assert.NoError(t, err)
	assert.Len(t, queries, 1)

	expectedQuery := `
MERGE INTO "database"."schema"."table" AS tgt
USING SELECT * FROM staging AS stg ON tgt."id" = stg."id"
WHEN MATCHED AND COALESCE(stg."__artie_only_set_delete", false) = false THEN UPDATE SET "id"=stg."id","name"=stg."name","__artie_delete"=stg."__artie_delete"
WHEN MATCHED AND COALESCE(stg."__artie_only_set_delete", false) = true THEN UPDATE SET "__artie_delete"=stg."__artie_delete"
WHEN NOT MATCHED THEN INSERT ("id","name","__artie_delete") VALUES (stg."id",stg."name",stg."__artie_delete")`
	assert.Equal(t, expectedQuery, queries[0])

	// Test regular mode
	queries, err = dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, nil, cols, false, true)
	assert.NoError(t, err)
	assert.Len(t, queries, 1)

	expectedQuery = `
MERGE INTO "database"."schema"."table" AS tgt USING SELECT * FROM staging AS stg ON tgt."id" = stg."id"
WHEN MATCHED AND stg."__artie_delete" = true THEN DELETE
WHEN MATCHED AND COALESCE(stg."__artie_delete", false) = false THEN UPDATE SET "id"=stg."id","name"=stg."name"
WHEN NOT MATCHED AND COALESCE(stg."__artie_delete", false) = false THEN INSERT ("id","name") VALUES (stg."id",stg."name")`
	assert.Equal(t, expectedQuery, queries[0])

	// Test multiple primary keys
	multiplePrimaryKeys := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("tenant_id", typing.String),
	}
	multiKeyCols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("tenant_id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	queries, err = dialect.BuildMergeQueries(tableID, subQuery, multiplePrimaryKeys, nil, multiKeyCols, false, false)
	assert.NoError(t, err)
	assert.Len(t, queries, 1)
	assert.Contains(t, queries[0], `tgt."id" = stg."id" AND tgt."tenant_id" = stg."tenant_id"`)

	// Test with additional equality strings
	additionalEqualityStrings := []string{`"partition_date" = '2023-01-01'`}
	queries, err = dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, additionalEqualityStrings, cols, false, true)
	assert.NoError(t, err)
	assert.Len(t, queries, 1)
	assert.Contains(t, queries[0], `tgt."id" = stg."id" AND "partition_date" = '2023-01-01'`)
}

func TestPostgresDialect_BuildMergeQueries_DisableMerge(t *testing.T) {
	dialect := NewPostgresDialect(true)
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns(`"schema"."table"`)

	subQuery := `"schema"."table__temp"`
	primaryKeys := []columns.Column{
		columns.NewColumn("id", typing.String),
	}

	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	// Test regular mode with hard deletes
	queries, err := dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, nil, cols, false, true)
	assert.NoError(t, err)
	assert.Len(t, queries, 3)

	assert.Equal(t,
		`UPDATE "schema"."table" AS tgt SET "id"=stg."id","name"=stg."name" FROM "schema"."table__temp" AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_delete", false) = false;`,
		queries[0])

	assert.Equal(t,
		`INSERT INTO "schema"."table" ("id","name") SELECT stg."id",stg."name" FROM "schema"."table__temp" AS stg LEFT JOIN "schema"."table" AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL AND COALESCE(stg."__artie_delete", false) = false;`,
		queries[1])

	assert.Equal(t,
		`DELETE FROM "schema"."table" AS tgt USING "schema"."table__temp" AS stg WHERE tgt."id" = stg."id" AND stg."__artie_delete" = true;`,
		queries[2])

	// Test regular mode without hard deletes
	queries, err = dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, nil, cols, false, false)
	assert.NoError(t, err)
	assert.Len(t, queries, 2)

	// Test soft delete mode
	queries, err = dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, nil, cols, true, false)
	assert.NoError(t, err)
	assert.Len(t, queries, 3)

	assert.Equal(t,
		`UPDATE "schema"."table" AS tgt SET "id"=stg."id","name"=stg."name","__artie_delete"=stg."__artie_delete" FROM "schema"."table__temp" AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_only_set_delete", false) = false;`,
		queries[0])

	assert.Equal(t,
		`UPDATE "schema"."table" AS tgt SET "__artie_delete"=stg."__artie_delete" FROM "schema"."table__temp" AS stg WHERE tgt."id" = stg."id" AND COALESCE(stg."__artie_only_set_delete", false) = true;`,
		queries[1])

	assert.Equal(t,
		`INSERT INTO "schema"."table" ("id","name","__artie_delete") SELECT stg."id",stg."name",stg."__artie_delete" FROM "schema"."table__temp" AS stg LEFT JOIN "schema"."table" AS tgt ON tgt."id" = stg."id" WHERE tgt."id" IS NULL;`,
		queries[2])
}

func TestPostgresDialect_BuildMergeQueries_DisableMerge_CompositeKey(t *testing.T) {
	dialect := NewPostgresDialect(true)
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns(`"schema"."table"`)

	subQuery := `"schema"."table__temp"`
	primaryKeys := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("tenant_id", typing.String),
	}

	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("tenant_id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	queries, err := dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, nil, cols, false, true)
	assert.NoError(t, err)
	assert.Len(t, queries, 3)

	assert.Contains(t, queries[0], `tgt."id" = stg."id" AND tgt."tenant_id" = stg."tenant_id"`)
	assert.Contains(t, queries[1], `tgt."id" = stg."id" AND tgt."tenant_id" = stg."tenant_id"`)
	assert.Contains(t, queries[1], `COALESCE(stg."__artie_delete", false) = false`)
	assert.Contains(t, queries[2], `tgt."id" = stg."id" AND tgt."tenant_id" = stg."tenant_id"`)
}

func TestPostgresDialect_BuildMergeQueries_DisableMerge_AdditionalEqualityStrings(t *testing.T) {
	dialect := NewPostgresDialect(true)
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns(`"schema"."table"`)

	subQuery := `"schema"."table__temp"`
	primaryKeys := []columns.Column{columns.NewColumn("id", typing.String)}
	// Use production-like predicates with tgt and stg aliases (as produced by BuildAdditionalEqualityStrings)
	additionalEqualityStrings := []string{`tgt."partition_date" = stg."partition_date"`}
	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	// Test with hard deletes
	queries, err := dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, additionalEqualityStrings, cols, false, true)
	assert.NoError(t, err)
	assert.Len(t, queries, 3)

	// UPDATE should include the additional equality string
	assert.Contains(t, queries[0], `tgt."id" = stg."id" AND tgt."partition_date" = stg."partition_date"`)
	// INSERT should include the additional equality string in the JOIN condition
	assert.Contains(t, queries[1], `tgt."id" = stg."id" AND tgt."partition_date" = stg."partition_date"`)
	// DELETE should include the additional equality string (this was the bug - tgt alias must be defined)
	assert.Contains(t, queries[2], `tgt."partition_date" = stg."partition_date"`)
	assert.Contains(t, queries[2], `DELETE FROM "schema"."table" AS tgt USING`)

	// Test soft delete mode
	queries, err = dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, additionalEqualityStrings, cols, true, false)
	assert.NoError(t, err)
	assert.Len(t, queries, 3)

	// Both UPDATE queries should include the additional equality string
	assert.Contains(t, queries[0], `tgt."id" = stg."id" AND tgt."partition_date" = stg."partition_date"`)
	assert.Contains(t, queries[1], `tgt."id" = stg."id" AND tgt."partition_date" = stg."partition_date"`)
	// INSERT should include the additional equality string
	assert.Contains(t, queries[2], `tgt."id" = stg."id" AND tgt."partition_date" = stg."partition_date"`)
}

func TestPostgresDialect_BuildMergeQueries_DisableMerge_ToastColumns(t *testing.T) {
	dialect := NewPostgresDialect(true)
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns(`"schema"."table"`)

	subQuery := `"schema"."table__temp"`
	primaryKeys := []columns.Column{
		columns.NewColumn("id", typing.String),
	}

	// Create columns with TOAST columns (like in Redshift tests)
	idCol := columns.NewColumn("id", typing.String)
	nameCol := columns.NewColumn("name", typing.String)

	// Regular string column - not TOAST
	emailCol := columns.NewColumn("email", typing.String)
	emailCol.ToastColumn = false

	// TOAST-able text column
	toastTextCol := columns.NewColumn("toast_text", typing.String)
	toastTextCol.ToastColumn = true

	// TOAST-able JSONB column
	toastJsonCol := columns.NewColumn("json_data", typing.Struct)
	toastJsonCol.ToastColumn = true

	cols := []columns.Column{
		idCol,
		nameCol,
		emailCol,
		toastTextCol,
		toastJsonCol,
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	// Test regular mode with hard deletes
	queries, err := dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, nil, cols, false, true)
	assert.NoError(t, err)
	assert.Len(t, queries, 3)

	// UPDATE query should have TOAST column handling with CASE WHEN for toast columns
	updateQuery := queries[0]
	// Regular columns should be simple assignments
	assert.Contains(t, updateQuery, `"id"=stg."id"`)
	assert.Contains(t, updateQuery, `"name"=stg."name"`)
	assert.Contains(t, updateQuery, `"email"=stg."email"`)
	// TOAST text column should have CASE WHEN with NOT LIKE check
	assert.Contains(t, updateQuery, `"toast_text"= CASE WHEN COALESCE(stg."toast_text", '') NOT LIKE '%__debezium_unavailable_value%' THEN stg."toast_text" ELSE tgt."toast_text" END`)
	// TOAST JSONB column should have CASE WHEN with ::text cast and NOT LIKE check
	assert.Contains(t, updateQuery, `"json_data"= CASE WHEN COALESCE(stg."json_data"::text, '') NOT LIKE '%__debezium_unavailable_value%' THEN stg."json_data" ELSE tgt."json_data" END`)

	// INSERT query should include all columns (TOAST handling not needed for INSERT)
	insertQuery := queries[1]
	assert.Contains(t, insertQuery, `INSERT INTO "schema"."table"`)
	assert.Contains(t, insertQuery, `"toast_text"`)
	assert.Contains(t, insertQuery, `"json_data"`)

	// DELETE query should not be affected by TOAST columns
	deleteQuery := queries[2]
	assert.Contains(t, deleteQuery, `DELETE FROM "schema"."table" AS tgt USING`)
	assert.Contains(t, deleteQuery, `stg."__artie_delete" = true`)
}

func TestPostgresDialect_BuildMergeQueries_DisableMerge_ToastColumns_SoftDelete(t *testing.T) {
	dialect := NewPostgresDialect(true)
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns(`"schema"."table"`)

	subQuery := `"schema"."table__temp"`
	primaryKeys := []columns.Column{
		columns.NewColumn("id", typing.String),
	}

	// Create columns with TOAST columns
	idCol := columns.NewColumn("id", typing.String)
	nameCol := columns.NewColumn("name", typing.String)

	// TOAST-able text column
	toastTextCol := columns.NewColumn("toast_text", typing.String)
	toastTextCol.ToastColumn = true

	cols := []columns.Column{
		idCol,
		nameCol,
		toastTextCol,
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
		columns.NewColumn(constants.OnlySetDeleteColumnMarker, typing.Boolean),
	}

	// Test soft delete mode
	queries, err := dialect.BuildMergeQueries(tableID, subQuery, primaryKeys, nil, cols, true, false)
	assert.NoError(t, err)
	assert.Len(t, queries, 3)

	// First UPDATE query (all columns) should have TOAST column handling
	updateAllQuery := queries[0]
	assert.Contains(t, updateAllQuery, `"id"=stg."id"`)
	assert.Contains(t, updateAllQuery, `"name"=stg."name"`)
	assert.Contains(t, updateAllQuery, `"toast_text"= CASE WHEN COALESCE(stg."toast_text", '') NOT LIKE '%__debezium_unavailable_value%' THEN stg."toast_text" ELSE tgt."toast_text" END`)
	assert.Contains(t, updateAllQuery, `"__artie_delete"=stg."__artie_delete"`)
	assert.Contains(t, updateAllQuery, `COALESCE(stg."__artie_only_set_delete", false) = false`)

	// Second UPDATE query (only delete marker) should NOT have TOAST handling since it only updates __artie_delete
	updateDeleteOnlyQuery := queries[1]
	assert.Contains(t, updateDeleteOnlyQuery, `SET "__artie_delete"=stg."__artie_delete"`)
	assert.Contains(t, updateDeleteOnlyQuery, `COALESCE(stg."__artie_only_set_delete", false) = true`)
	// Should not contain TOAST column handling in this query
	assert.NotContains(t, updateDeleteOnlyQuery, `toast_text`)

	// INSERT query should include __artie_delete column for soft delete
	insertQuery := queries[2]
	assert.Contains(t, insertQuery, `"__artie_delete"`)
	assert.Contains(t, insertQuery, `"toast_text"`)
}
