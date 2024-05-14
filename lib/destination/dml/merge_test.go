package dml

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/mocks"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func TestMergeStatementSoftDelete(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	cols := []string{
		"id",
		"bar",
		"updated_at",
		constants.DeleteColumnMarker,
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', false)", "1", "456", time.Now().Round(0).Format(time.RFC3339)),
		fmt.Sprintf("('%s', '%s', '%v', true)", "2", "bb", time.Now().Round(0).Format(time.RFC3339)), // Delete row 2.
		fmt.Sprintf("('%s', '%s', '%v', false)", "3", "dd", time.Now().Round(0).Format(time.RFC3339)),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	var _cols columns.Columns
	_cols.AddColumn(columns.NewColumn("id", typing.String))
	_cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)
	for _, idempotentKey := range []string{"", "updated_at"} {
		mergeArg := MergeArgument{
			TableID:       fakeTableID,
			SubQuery:      subQuery,
			IdempotentKey: idempotentKey,
			PrimaryKeys:   []columns.Column{columns.NewColumn("id", typing.Invalid)},
			Columns:       _cols.ValidColumns(),
			Dialect:       snowflakeDialect.SnowflakeDialect{},
			SoftDelete:    true,
		}

		statements, err := mergeArg.buildDefaultStatements()
		assert.Len(t, statements, 1)
		mergeSQL := statements[0]
		assert.NoError(t, err)
		assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
		// Soft deletion flag being passed.
		assert.Contains(t, mergeSQL, `"__ARTIE_DELETE"=cc."__ARTIE_DELETE"`, mergeSQL)

		assert.Equal(t, len(idempotentKey) > 0, strings.Contains(mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at")))
	}
}

func TestMergeStatement(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	colToTypes := map[string]typing.KindDetails{
		"id":                         typing.String,
		"bar":                        typing.String,
		"updated_at":                 typing.String,
		"start":                      typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	// This feels a bit round about, but this is because iterating over a map is not deterministic.
	cols := []string{"id", "bar", "updated_at", "start", constants.DeleteColumnMarker}
	var _cols columns.Columns
	for _, col := range cols {
		_cols.AddColumn(columns.NewColumn(col, colToTypes[col]))
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "1", "456", "foo", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "2", "bb", "bar", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "3", "dd", "world", time.Now().Round(0).UTC()),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)
	mergeArg := MergeArgument{
		TableID:       fakeTableID,
		SubQuery:      subQuery,
		IdempotentKey: "",
		PrimaryKeys:   []columns.Column{columns.NewColumn("id", typing.Invalid)},
		Columns:       _cols.ValidColumns(),
		Dialect:       snowflakeDialect.SnowflakeDialect{},
		SoftDelete:    false,
	}

	statements, err := mergeArg.buildDefaultStatements()
	assert.Len(t, statements, 1)
	mergeSQL := statements[0]
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.NotContains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", `"UPDATED_AT"`, `"UPDATED_AT"`), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	// Check primary keys clause
	assert.Contains(t, mergeSQL, `AS cc ON c."ID" = cc."ID"`, mergeSQL)

	// Check setting for update
	assert.Contains(t, mergeSQL, `SET "ID"=cc."ID","BAR"=cc."BAR","UPDATED_AT"=cc."UPDATED_AT","START"=cc."START"`, mergeSQL)
	// Check for INSERT
	assert.Contains(t, mergeSQL, `"ID","BAR","UPDATED_AT","START"`, mergeSQL)
	assert.Contains(t, mergeSQL, `cc."ID",cc."BAR",cc."UPDATED_AT",cc."START"`, mergeSQL)
}

func TestMergeStatementIdempotentKey(t *testing.T) {
	fqTable := "database.schema.table"
	cols := []string{
		"id",
		"bar",
		"updated_at",
		constants.DeleteColumnMarker,
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', false)", "1", "456", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', false)", "2", "bb", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', false)", "3", "dd", time.Now().Round(0).UTC()),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	var _cols columns.Columns
	_cols.AddColumn(columns.NewColumn("id", typing.String))
	_cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)
	mergeArg := MergeArgument{
		TableID:       fakeTableID,
		SubQuery:      subQuery,
		IdempotentKey: "updated_at",
		PrimaryKeys:   []columns.Column{columns.NewColumn("id", typing.Invalid)},
		Columns:       _cols.ValidColumns(),
		Dialect:       snowflakeDialect.SnowflakeDialect{},
		SoftDelete:    false,
	}

	statements, err := mergeArg.buildDefaultStatements()
	assert.Len(t, statements, 1)
	mergeSQL := statements[0]
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.Contains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
}

func TestMergeStatementCompositeKey(t *testing.T) {
	fqTable := "database.schema.table"
	cols := []string{
		"id",
		"another_id",
		"bar",
		"updated_at",
		constants.DeleteColumnMarker,
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "1", "3", "456", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "2", "2", "bb", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%s', '%v', false)", "3", "1", "dd", time.Now().Round(0).UTC()),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	var _cols columns.Columns
	_cols.AddColumn(columns.NewColumn("id", typing.String))
	_cols.AddColumn(columns.NewColumn("another_id", typing.String))
	_cols.AddColumn(columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)
	mergeArg := MergeArgument{
		TableID:       fakeTableID,
		SubQuery:      subQuery,
		IdempotentKey: "updated_at",
		PrimaryKeys: []columns.Column{
			columns.NewColumn("id", typing.Invalid),
			columns.NewColumn("another_id", typing.Invalid),
		},
		Columns:    _cols.ValidColumns(),
		Dialect:    snowflakeDialect.SnowflakeDialect{},
		SoftDelete: false,
	}

	statements, err := mergeArg.buildDefaultStatements()
	assert.Len(t, statements, 1)
	mergeSQL := statements[0]
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.Contains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	assert.Contains(t, mergeSQL, `cc ON c."ID" = cc."ID" and c."ANOTHER_ID" = cc."ANOTHER_ID"`, mergeSQL)
}

func TestMergeStatementEscapePrimaryKeys(t *testing.T) {
	// No idempotent key
	fqTable := "database.schema.table"
	colToTypes := map[string]typing.KindDetails{
		"id":                         typing.String,
		"group":                      typing.String,
		"updated_at":                 typing.String,
		"start":                      typing.String,
		constants.DeleteColumnMarker: typing.Boolean,
	}

	// This feels a bit round about, but this is because iterating over a map is not deterministic.
	cols := []string{"id", "group", "updated_at", "start", constants.DeleteColumnMarker}
	var _cols columns.Columns
	for _, col := range cols {
		_cols.AddColumn(columns.NewColumn(col, colToTypes[col]))
	}

	tableValues := []string{
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "1", "456", "foo", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "2", "bb", "bar", time.Now().Round(0).UTC()),
		fmt.Sprintf("('%s', '%s', '%v', '%v', false)", "3", "dd", "world", time.Now().Round(0).UTC()),
	}

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	fakeTableID := &mocks.FakeTableIdentifier{}
	fakeTableID.FullyQualifiedNameReturns(fqTable)
	mergeArg := MergeArgument{
		TableID:       fakeTableID,
		SubQuery:      subQuery,
		IdempotentKey: "",
		PrimaryKeys: []columns.Column{
			columns.NewColumn("id", typing.Invalid),
			columns.NewColumn("group", typing.Invalid),
		},
		Columns:    _cols.ValidColumns(),
		Dialect:    snowflakeDialect.SnowflakeDialect{},
		SoftDelete: false,
	}

	statements, err := mergeArg.buildDefaultStatements()
	assert.Len(t, statements, 1)
	mergeSQL := statements[0]
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.NotContains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", `"UPDATED_AT"`, `"UPDATED_AT"`), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	// Check primary keys clause
	assert.Contains(t, mergeSQL, `AS cc ON c."ID" = cc."ID" and c."GROUP" = cc."GROUP"`, mergeSQL)
	// Check setting for update
	assert.Contains(t, mergeSQL, `SET "ID"=cc."ID","GROUP"=cc."GROUP","UPDATED_AT"=cc."UPDATED_AT","START"=cc."START"`, mergeSQL)
	// Check for INSERT
	assert.Contains(t, mergeSQL, `"ID","GROUP","UPDATED_AT","START"`, mergeSQL)
	assert.Contains(t, mergeSQL, `cc."ID",cc."GROUP",cc."UPDATED_AT",cc."START"`, mergeSQL)
}

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
