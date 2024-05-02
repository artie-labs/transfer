package dml

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
	"github.com/stretchr/testify/assert"
)

func Test_GetMSSQLStatement(t *testing.T) {
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
		Columns:       &_cols,
		DestKind:      constants.MSSQL,
		Dialect:       sql.MSSQLDialect{},
		SoftDelete:    false,
	}

	mergeSQL, err := mergeArg.GetMSSQLStatement()
	assert.NoError(t, err)
	assert.Contains(t, mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable), mergeSQL)
	assert.NotContains(t, mergeSQL, fmt.Sprintf(`cc."%s" >= c."%s"`, "updated_at", "updated_at"), fmt.Sprintf("Idempotency key: %s", mergeSQL))
	// Check primary keys clause
	assert.Contains(t, mergeSQL, `AS cc ON c."id" = cc."id"`, mergeSQL)

	assert.Contains(t, mergeSQL, `SET "id"=cc."id","bar"=cc."bar","updated_at"=cc."updated_at","start"=cc."start"`, mergeSQL)
	assert.Contains(t, mergeSQL, `id,bar,updated_at,start`, mergeSQL)
	assert.Contains(t, mergeSQL, `cc."id",cc."bar",cc."updated_at",cc."start"`, mergeSQL)
}
