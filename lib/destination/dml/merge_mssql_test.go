package dml

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func Test_BuildMSSQLStatement(t *testing.T) {
	fqTable := "database.schema.table"

	var _cols = []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn("bar", typing.String),
		columns.NewColumn("updated_at", typing.String),
		columns.NewColumn("start", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
	}
	cols := make([]string, len(_cols))
	for i, col := range _cols {
		cols[i] = col.Name()
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
		Columns:       _cols,
		Dialect:       sql.MSSQLDialect{},
		SoftDelete:    false,
	}

	mergeSQL, err := mergeArg.buildMSSQLStatement()
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.NotContains(t, mergeSQL, fmt.Sprintf(`cc."%s" >= c."%s"`, "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	// Check primary keys clause
	assert.Contains(t, mergeSQL, `AS cc ON c."id" = cc."id"`, mergeSQL)

	assert.Contains(t, mergeSQL, `SET "id"=cc."id","bar"=cc."bar","updated_at"=cc."updated_at","start"=cc."start"`, mergeSQL)
	assert.Contains(t, mergeSQL, `id,bar,updated_at,start`, mergeSQL)
	assert.Contains(t, mergeSQL, `cc."id",cc."bar",cc."updated_at",cc."start"`, mergeSQL)
}

func TestMergeArgument_BuildStatements_MSSQL(t *testing.T) {
	var cols = []columns.Column{
		columns.NewColumn("id", typing.String),
		columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean),
	}

	mergeArg := MergeArgument{
		TableID:             MockTableIdentifier{"database.schema.table"},
		SubQuery:            "{SUB_QUERY}",
		IdempotentKey:       "",
		PrimaryKeys:         []columns.Column{cols[0]},
		Columns:             cols,
		Dialect:             sql.MSSQLDialect{},
		ContainsHardDeletes: ptr.ToBool(true),
		SoftDelete:          true,
	}

	statement, err := mergeArg.buildMSSQLStatement()
	assert.NoError(t, err)
	statements, err := mergeArg.BuildStatements()
	assert.NoError(t, err)
	assert.Equal(t, statements, []string{statement})
}
