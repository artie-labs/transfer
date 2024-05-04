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

func TestMergeArgument_RedshiftEqualitySQLParts(t *testing.T) {
	mergeArg := MergeArgument{
		PrimaryKeys: []columns.Column{columns.NewColumn("col1", typing.Invalid), columns.NewColumn("col2", typing.Invalid)},
		Dialect:     sql.RedshiftDialect{},
	}
	assert.Equal(t, []string{`c."col1" = cc."col1"`, `c."col2" = cc."col2"`}, mergeArg.redshiftEqualitySQLParts())
}

func TestMergeArgument_BuildRedshiftInsertQuery(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	mergeArg := MergeArgument{
		TableID:     MockTableIdentifier{"{TABLE_ID}"},
		SubQuery:    "{SUB_QUERY}",
		PrimaryKeys: []columns.Column{cols[0], cols[2]},
		Dialect:     sql.RedshiftDialect{},
	}
	assert.Equal(t,
		`INSERT INTO {TABLE_ID} ("col1","col2","col3") SELECT cc."col1",cc."col2",cc."col3" FROM {SUB_QUERY} AS cc LEFT JOIN {TABLE_ID} AS c ON c."col1" = cc."col1" AND c."col3" = cc."col3" WHERE c."col1" IS NULL;`,
		mergeArg.buildRedshiftInsertQuery(cols),
	)
}

func TestMergeArgument_BuildRedshiftUpdateQuery(t *testing.T) {
	testCases := []struct {
		name          string
		softDelete    bool
		idempotentKey string
		expected      string
	}{
		{
			name:       "soft delete enabled",
			softDelete: true,
			expected:   `UPDATE {TABLE_ID} AS c SET "col1"=cc."col1","col2"=cc."col2","col3"=cc."col3" FROM {SUB_QUERY} AS cc WHERE c."col1" = cc."col1" AND c."col3" = cc."col3";`,
		},
		{
			name:          "soft delete enabled + idempotent key",
			softDelete:    true,
			idempotentKey: "{ID_KEY}",
			expected:      `UPDATE {TABLE_ID} AS c SET "col1"=cc."col1","col2"=cc."col2","col3"=cc."col3" FROM {SUB_QUERY} AS cc WHERE c."col1" = cc."col1" AND c."col3" = cc."col3" AND cc.{ID_KEY} >= c.{ID_KEY};`,
		},
		{
			name:       "soft delete disabled",
			softDelete: false,
			expected:   `UPDATE {TABLE_ID} AS c SET "col1"=cc."col1","col2"=cc."col2","col3"=cc."col3" FROM {SUB_QUERY} AS cc WHERE c."col1" = cc."col1" AND c."col3" = cc."col3" AND COALESCE(cc."__artie_delete", false) = false;`,
		},
		{
			name:          "soft delete disabled + idempotent key",
			softDelete:    false,
			idempotentKey: "{ID_KEY}",
			expected:      `UPDATE {TABLE_ID} AS c SET "col1"=cc."col1","col2"=cc."col2","col3"=cc."col3" FROM {SUB_QUERY} AS cc WHERE c."col1" = cc."col1" AND c."col3" = cc."col3" AND cc.{ID_KEY} >= c.{ID_KEY} AND COALESCE(cc."__artie_delete", false) = false;`,
		},
	}

	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	for _, testCase := range testCases {
		mergeArg := MergeArgument{
			TableID:       MockTableIdentifier{"{TABLE_ID}"},
			SubQuery:      "{SUB_QUERY}",
			PrimaryKeys:   []columns.Column{cols[0], cols[2]},
			Dialect:       sql.RedshiftDialect{},
			SoftDelete:    testCase.softDelete,
			IdempotentKey: testCase.idempotentKey,
		}
		assert.Equal(t, testCase.expected, mergeArg.buildRedshiftUpdateQuery(cols), testCase.name)
	}
}

func TestMergeArgument_BuildRedshiftDeleteQuery(t *testing.T) {
	cols := []columns.Column{
		columns.NewColumn("col1", typing.Invalid),
		columns.NewColumn("col2", typing.Invalid),
		columns.NewColumn("col3", typing.Invalid),
	}

	mergeArg := MergeArgument{
		TableID:     MockTableIdentifier{"{TABLE_ID}"},
		SubQuery:    "{SUB_QUERY}",
		PrimaryKeys: []columns.Column{cols[0], cols[1]},
		Dialect:     sql.RedshiftDialect{},
	}
	assert.Equal(t,
		`DELETE FROM {TABLE_ID} WHERE ("col1","col2") IN (SELECT cc."col1",cc."col2" FROM {SUB_QUERY} AS cc WHERE cc."__artie_delete" = true);`,
		mergeArg.buildRedshiftDeleteQuery(),
	)
}
