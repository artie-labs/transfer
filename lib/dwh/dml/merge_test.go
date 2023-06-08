package dml

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/stretchr/testify/assert"
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

	var _cols typing.Columns
	_cols.AddColumn(typing.NewColumn("id", typing.String))

	for _, idempotentKey := range []string{"", "updated_at"} {
		mergeSQL, err := MergeStatement(MergeArgument{
			FqTableName:    fqTable,
			SubQuery:       subQuery,
			IdempotentKey:  idempotentKey,
			PrimaryKeys:    []string{"id"},
			Columns:        cols,
			ColumnsToTypes: _cols,
			BigQuery:       false,
			SoftDelete:     true,
		})
		assert.NoError(t, err)
		assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable)), mergeSQL)
		// Soft deletion flag being passed.
		assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("%s=cc.%s", constants.DeleteColumnMarker, constants.DeleteColumnMarker)), mergeSQL)

		assert.Equal(t, len(idempotentKey) > 0, strings.Contains(mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at")))
	}

}

func TestMergeStatement(t *testing.T) {
	// No idempotent key
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

	var _cols typing.Columns
	_cols.AddColumn(typing.NewColumn("id", typing.String))

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))
	mergeSQL, err := MergeStatement(MergeArgument{
		FqTableName:    fqTable,
		SubQuery:       subQuery,
		IdempotentKey:  "",
		PrimaryKeys:    []string{"id"},
		Columns:        cols,
		ColumnsToTypes: _cols,
		BigQuery:       false,
		SoftDelete:     false,
	})
	assert.NoError(t, err)
	assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable)), mergeSQL)
	assert.False(t, strings.Contains(mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at")), fmt.Sprintf("Idempotency key: %s", mergeSQL))
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

	var _cols typing.Columns
	_cols.AddColumn(typing.NewColumn("id", typing.String))

	mergeSQL, err := MergeStatement(MergeArgument{
		FqTableName:    fqTable,
		SubQuery:       subQuery,
		IdempotentKey:  "updated_at",
		PrimaryKeys:    []string{"id"},
		Columns:        cols,
		ColumnsToTypes: _cols,
		BigQuery:       false,
		SoftDelete:     false,
	})
	assert.NoError(t, err)
	assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable)), mergeSQL)
	assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at")), fmt.Sprintf("Idempotency key: %s", mergeSQL))
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

	var _cols typing.Columns
	_cols.AddColumn(typing.NewColumn("id", typing.String))
	_cols.AddColumn(typing.NewColumn("another_id", typing.String))

	mergeSQL, err := MergeStatement(MergeArgument{
		FqTableName:    fqTable,
		SubQuery:       subQuery,
		IdempotentKey:  "updated_at",
		PrimaryKeys:    []string{"id", "another_id"},
		Columns:        cols,
		ColumnsToTypes: _cols,
		BigQuery:       false,
		SoftDelete:     false,
	})
	assert.NoError(t, err)
	assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable)), mergeSQL)
	assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at")), fmt.Sprintf("Idempotency key: %s", mergeSQL))

	assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("cc on c.id = cc.id and c.another_id = cc.another_id")))
}
