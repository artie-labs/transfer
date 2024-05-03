package dml

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// Have to mock a [types.TableIdentifier] otherwise we get circular imports.
type MockTableIdentifier struct {
	fqName string
}

func (m MockTableIdentifier) Table() string {
	panic("not implemented")
}

func (m MockTableIdentifier) WithTable(_ string) types.TableIdentifier {
	panic("not implemented")
}

func (m MockTableIdentifier) FullyQualifiedName() string {
	return m.fqName
}

func TestRemoveDeleteColumnMarker(t *testing.T) {
	col1 := columns.NewColumn("a", typing.Invalid)
	col2 := columns.NewColumn("b", typing.Invalid)
	col3 := columns.NewColumn("c", typing.Invalid)
	deleteColumnMarkerCol := columns.NewColumn(constants.DeleteColumnMarker, typing.Invalid)

	{
		result, removed := removeDeleteColumnMarker([]columns.Column{})
		assert.Empty(t, result)
		assert.False(t, removed)
	}
	{
		result, removed := removeDeleteColumnMarker([]columns.Column{col1})
		assert.Equal(t, []columns.Column{col1}, result)
		assert.False(t, removed)
	}
	{
		result, removed := removeDeleteColumnMarker([]columns.Column{col1, col2})
		assert.Equal(t, []columns.Column{col1, col2}, result)
		assert.False(t, removed)
	}
	{
		result, removed := removeDeleteColumnMarker([]columns.Column{deleteColumnMarkerCol})
		assert.True(t, removed)
		assert.Empty(t, result)
	}
	{
		result, removed := removeDeleteColumnMarker([]columns.Column{col1, deleteColumnMarkerCol, col2})
		assert.True(t, removed)
		assert.Equal(t, []columns.Column{col1, col2}, result)
	}
	{
		result, removed := removeDeleteColumnMarker([]columns.Column{col1, deleteColumnMarkerCol, col2, deleteColumnMarkerCol, col3})
		assert.True(t, removed)
		assert.Equal(t, []columns.Column{col1, col2, col3}, result)
	}
}

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

	for _, idempotentKey := range []string{"", "updated_at"} {
		mergeArg := MergeArgument{
			TableID:       MockTableIdentifier{fqTable},
			SubQuery:      subQuery,
			IdempotentKey: idempotentKey,
			PrimaryKeys:   []columns.Column{columns.NewColumn("id", typing.Invalid)},
			Columns:       _cols.ValidColumns(),
			DestKind:      constants.Snowflake,
			Dialect:       sql.SnowflakeDialect{},
			SoftDelete:    true,
		}

		mergeSQL, err := mergeArg.GetStatement()
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

	mergeArg := MergeArgument{
		TableID:       MockTableIdentifier{fqTable},
		SubQuery:      subQuery,
		IdempotentKey: "",
		PrimaryKeys:   []columns.Column{columns.NewColumn("id", typing.Invalid)},
		Columns:       _cols.ValidColumns(),
		DestKind:      constants.Snowflake,
		Dialect:       sql.SnowflakeDialect{},
		SoftDelete:    false,
	}

	mergeSQL, err := mergeArg.GetStatement()
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

	mergeArg := MergeArgument{
		TableID:       MockTableIdentifier{fqTable},
		SubQuery:      subQuery,
		IdempotentKey: "updated_at",
		PrimaryKeys:   []columns.Column{columns.NewColumn("id", typing.Invalid)},
		Columns:       _cols.ValidColumns(),
		DestKind:      constants.Snowflake,
		Dialect:       sql.SnowflakeDialect{},
		SoftDelete:    false,
	}

	mergeSQL, err := mergeArg.GetStatement()
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

	mergeArg := MergeArgument{
		TableID:       MockTableIdentifier{fqTable},
		SubQuery:      subQuery,
		IdempotentKey: "updated_at",
		PrimaryKeys: []columns.Column{
			columns.NewColumn("id", typing.Invalid),
			columns.NewColumn("another_id", typing.Invalid),
		},
		Columns:    _cols.ValidColumns(),
		DestKind:   constants.Snowflake,
		Dialect:    sql.SnowflakeDialect{},
		SoftDelete: false,
	}

	mergeSQL, err := mergeArg.GetStatement()
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

	mergeArg := MergeArgument{
		TableID:       MockTableIdentifier{fqTable},
		SubQuery:      subQuery,
		IdempotentKey: "",
		PrimaryKeys: []columns.Column{
			columns.NewColumn("id", typing.Invalid),
			columns.NewColumn("group", typing.Invalid),
		},
		Columns:    _cols.ValidColumns(),
		DestKind:   constants.Snowflake,
		Dialect:    sql.SnowflakeDialect{},
		SoftDelete: false,
	}

	mergeSQL, err := mergeArg.GetStatement()
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

func TestBuildInsertQuery(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
	}

	mergeArg := MergeArgument{
		TableID:     MockTableIdentifier{"{TABLE_ID}"},
		SubQuery:    "{SUB_QUERY}",
		PrimaryKeys: []columns.Column{cols[0], columns.NewColumn("othercol", typing.Invalid)},
		Dialect:     sql.SnowflakeDialect{},
	}
	assert.Equal(t,
		`INSERT INTO {TABLE_ID} ("COL1","COL2") SELECT cc."COL1",cc."COL2" FROM {SUB_QUERY} as cc LEFT JOIN {TABLE_ID} as c on {EQUALITY_PART_1} and {EQUALITY_PART_2} WHERE c."COL1" IS NULL;`,
		mergeArg.buildRedshiftInsertQuery(cols, []string{"{EQUALITY_PART_1}", "{EQUALITY_PART_2}"}),
	)
}
