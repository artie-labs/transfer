package dml

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
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

	for _, idempotentKey := range []string{"", "updated_at"} {
		mergeSQL, err := MergeStatement(fqTable, subQuery, "id", idempotentKey, cols, true)
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

	// select cc.foo, cc.bar from (values (12, 34), (44, 55)) as cc(foo, bar);
	subQuery := fmt.Sprintf("SELECT %s from (values %s) as %s(%s)",
		strings.Join(cols, ","), strings.Join(tableValues, ","), "_tbl", strings.Join(cols, ","))

	mergeSQL, err := MergeStatement(fqTable, subQuery, "id", "", cols, false)
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

	mergeSQL, err := MergeStatement(fqTable, subQuery, "id", "updated_at", cols, false)
	assert.NoError(t, err)
	assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("MERGE INTO %s", fqTable)), mergeSQL)
	assert.True(t, strings.Contains(mergeSQL, fmt.Sprintf("cc.%s >= c.%s", "updated_at", "updated_at")), fmt.Sprintf("Idempotency key: %s", mergeSQL))
}
