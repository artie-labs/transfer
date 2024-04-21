package dml

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/ptr"
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
			TableID:           MockTableIdentifier{fqTable},
			SubQuery:          subQuery,
			IdempotentKey:     idempotentKey,
			PrimaryKeys:       []columns.Wrapper{columns.NewWrapper(columns.NewColumn("id", typing.Invalid), false, nil)},
			Columns:           &_cols,
			DestKind:          constants.Snowflake,
			SoftDelete:        true,
			UppercaseEscNames: ptr.ToBool(false),
		}

		mergeSQL, err := mergeArg.GetStatement()
		assert.NoError(t, err)
		assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
		// Soft deletion flag being passed.
		assert.Contains(t, mergeSQL, fmt.Sprintf("%s=cc.%s", constants.DeleteColumnMarker, constants.DeleteColumnMarker), mergeSQL)

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
		TableID:           MockTableIdentifier{fqTable},
		SubQuery:          subQuery,
		IdempotentKey:     "",
		PrimaryKeys:       []columns.Wrapper{columns.NewWrapper(columns.NewColumn("id", typing.Invalid), false, nil)},
		Columns:           &_cols,
		DestKind:          constants.Snowflake,
		SoftDelete:        false,
		UppercaseEscNames: ptr.ToBool(false),
	}

	mergeSQL, err := mergeArg.GetStatement()
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.NotContains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	// Check primary keys clause
	assert.Contains(t, mergeSQL, "AS cc ON c.id = cc.id", mergeSQL)

	// Check setting for update
	assert.Contains(t, mergeSQL, `SET id=cc.id,bar=cc.bar,updated_at=cc.updated_at,"start"=cc."start"`, mergeSQL)
	// Check for INSERT
	assert.Contains(t, mergeSQL, `id,bar,updated_at,"start"`, mergeSQL)
	assert.Contains(t, mergeSQL, `cc.id,cc.bar,cc.updated_at,cc."start"`, mergeSQL)
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
		TableID:           MockTableIdentifier{fqTable},
		SubQuery:          subQuery,
		IdempotentKey:     "updated_at",
		PrimaryKeys:       []columns.Wrapper{columns.NewWrapper(columns.NewColumn("id", typing.Invalid), false, nil)},
		Columns:           &_cols,
		DestKind:          constants.Snowflake,
		SoftDelete:        false,
		UppercaseEscNames: ptr.ToBool(false),
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
		PrimaryKeys: []columns.Wrapper{columns.NewWrapper(columns.NewColumn("id", typing.Invalid), false, nil),
			columns.NewWrapper(columns.NewColumn("another_id", typing.Invalid), false, nil)},
		Columns:           &_cols,
		DestKind:          constants.Snowflake,
		SoftDelete:        false,
		UppercaseEscNames: ptr.ToBool(false),
	}

	mergeSQL, err := mergeArg.GetStatement()
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.Contains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	assert.Contains(t, mergeSQL, "cc ON c.id = cc.id and c.another_id = cc.another_id", mergeSQL)
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
		PrimaryKeys: []columns.Wrapper{
			columns.NewWrapper(columns.NewColumn("id", typing.Invalid), false, &sql.NameArgs{
				Escape:   true,
				DestKind: constants.Snowflake,
			}),
			columns.NewWrapper(columns.NewColumn("group", typing.Invalid), false, &sql.NameArgs{
				Escape:   true,
				DestKind: constants.Snowflake,
			}),
		},
		Columns:           &_cols,
		DestKind:          constants.Snowflake,
		SoftDelete:        false,
		UppercaseEscNames: ptr.ToBool(false),
	}

	mergeSQL, err := mergeArg.GetStatement()
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.NotContains(t, mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	// Check primary keys clause
	assert.Contains(t, mergeSQL, `AS cc ON c.id = cc.id and c."group" = cc."group"`, mergeSQL)
	// Check setting for update
	assert.Contains(t, mergeSQL, `SET id=cc.id,"group"=cc."group",updated_at=cc.updated_at,"start"=cc."start"`, mergeSQL)
	// Check for INSERT
	assert.Contains(t, mergeSQL, `id,"group",updated_at,"start"`, mergeSQL)
	assert.Contains(t, mergeSQL, `cc.id,cc."group",cc.updated_at,cc."start"`, mergeSQL)
}
