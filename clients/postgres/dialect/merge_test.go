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
