package dialect

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestMSSQLDialect_BuildInsertQuery(t *testing.T) {
	dialect := MSSQLDialect{}
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns("database.schema.table")

	subQuery := "SELECT * FROM staging"
	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
	}
	joinOn := "tgt.id = stg.id"
	pk := columns.NewColumn("id", typing.String)

	// Test the function
	query := dialect.buildInsertQuery(tableID, subQuery, cols, joinOn, pk)

	// Verify the result
	expectedQuery := `
INSERT INTO database.schema.table ([id],[name],[__artie_delete])
SELECT stg.[id],stg.[name],stg.[__artie_delete] FROM SELECT * FROM staging AS stg
LEFT JOIN database.schema.table AS tgt ON tgt.id = stg.id
WHERE tgt.[id] IS NULL;`
	assert.Equal(t, expectedQuery, query)
}

func TestMSSQLDialect_BuildUpdateAllColumnsQuery(t *testing.T) {
	dialect := MSSQLDialect{}
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns("database.schema.table")

	subQuery := "SELECT * FROM staging"
	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
	}
	joinOn := "tgt.id = stg.id"

	// Test the function
	query := dialect.buildUpdateAllColumnsQuery(tableID, subQuery, cols, joinOn)

	// Verify the result
	expectedQuery := `
UPDATE tgt SET [id]=stg.[id],[name]=stg.[name],[__artie_delete]=stg.[__artie_delete]
FROM SELECT * FROM staging AS stg LEFT JOIN database.schema.table AS tgt ON tgt.id = stg.id
WHERE COALESCE(stg.[__artie_only_set_delete], 0) = 0;`

	assert.Equal(t, expectedQuery, query)
}

func TestMSSQLDialect_BuildUpdateDeleteColumnQuery(t *testing.T) {
	dialect := MSSQLDialect{}
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns("database.schema.table")

	subQuery := "SELECT * FROM staging"
	joinOn := "tgt.id = stg.id"

	// Test the function
	query := dialect.buildUpdateDeleteColumnQuery(tableID, subQuery, joinOn)

	// Verify the result
	expectedQuery := `
UPDATE tgt SET [__artie_delete]=stg.[__artie_delete]
FROM SELECT * FROM staging AS stg LEFT JOIN database.schema.table AS tgt ON tgt.id = stg.id
WHERE COALESCE(stg.[__artie_only_set_delete], 0) = 1;`

	assert.Equal(t, expectedQuery, query)
}

func TestMSSQLDialect_BuildRegularMergeQueries(t *testing.T) {
	dialect := MSSQLDialect{}
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns("database.schema.table")

	subQuery := "SELECT * FROM staging"
	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
	}
	joinOn := "tgt.id = stg.id"

	// Test the function
	queries, err := dialect.buildRegularMergeQueries(tableID, subQuery, cols, joinOn)

	// Verify the result
	assert.NoError(t, err)
	assert.Len(t, queries, 1)

	expectedQuery := `
MERGE INTO database.schema.table tgt
USING SELECT * FROM staging AS stg ON tgt.id = stg.id
WHEN MATCHED AND stg.[__artie_delete] = 1 THEN DELETE
WHEN MATCHED AND COALESCE(stg.[__artie_delete], 0) = 0 THEN UPDATE SET [id]=stg.[id],[name]=stg.[name]
WHEN NOT MATCHED AND COALESCE(stg.[__artie_delete], 1) = 0 THEN INSERT ([id],[name]) VALUES (stg.[id],stg.[name]);`

	assert.Equal(t, expectedQuery, queries[0])
}

func TestMSSQLDialect_BuildSoftDeleteMergeQueries(t *testing.T) {
	dialect := MSSQLDialect{}
	tableID := &mocks.FakeTableIdentifier{}
	tableID.FullyQualifiedNameReturns("database.schema.table")

	subQuery := "SELECT * FROM staging"
	primaryKeys := []columns.Column{
		columns.NewColumn("id", typing.String),
	}
	cols := []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("name", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
	}
	joinOn := "tgt.id = stg.id"

	// Test the function
	queries, err := dialect.buildSoftDeleteMergeQueries(tableID, subQuery, primaryKeys, cols, joinOn)

	// Verify the result
	assert.NoError(t, err)
	assert.Len(t, queries, 3)

	// The first query should be the insert query
	expectedInsertQuery := `
INSERT INTO database.schema.table ([id],[name],[__artie_delete])
SELECT stg.[id],stg.[name],stg.[__artie_delete] FROM SELECT * FROM staging AS stg
LEFT JOIN database.schema.table AS tgt ON tgt.id = stg.id
WHERE tgt.[id] IS NULL;`

	// The second query should be the update all columns query
	expectedUpdateAllColumnsQuery := `
UPDATE tgt SET [id]=stg.[id],[name]=stg.[name],[__artie_delete]=stg.[__artie_delete]
FROM SELECT * FROM staging AS stg LEFT JOIN database.schema.table AS tgt ON tgt.id = stg.id
WHERE COALESCE(stg.[__artie_only_set_delete], 0) = 0;`

	// The third query should be the update delete column query
	expectedUpdateDeleteColumnQuery := `
UPDATE tgt SET [__artie_delete]=stg.[__artie_delete]
FROM SELECT * FROM staging AS stg LEFT JOIN database.schema.table AS tgt ON tgt.id = stg.id
WHERE COALESCE(stg.[__artie_only_set_delete], 0) = 1;`

	assert.Equal(t, expectedInsertQuery, queries[0])
	assert.Equal(t, expectedUpdateAllColumnsQuery, queries[1])
	assert.Equal(t, expectedUpdateDeleteColumnQuery, queries[2])
}
