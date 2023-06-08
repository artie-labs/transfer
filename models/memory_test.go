package models

import (
	"context"
	"testing"

	"github.com/artie-labs/transfer/lib/optimization"

	"github.com/stretchr/testify/assert"
)

func TestTableData_Complete(t *testing.T) {
	ctx := context.Background()
	ctx = LoadMemoryDB(ctx)

	db := GetMemoryDB(ctx)
	tableName := "table"

	// TableData does not exist
	_, isOk := db.TableData()[tableName]
	assert.False(t, isOk)

	td := db.GetOrCreateTableData(tableName)
	assert.True(t, td.Empty())
	_, isOk = db.TableData()[tableName]
	assert.True(t, isOk)

	// Add the td struct
	td.TableData = &optimization.TableData{}
	assert.False(t, td.Empty())

	td.Wipe()
	assert.True(t, td.Empty())
}
