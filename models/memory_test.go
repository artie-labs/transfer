package models

import (
	"testing"

	"github.com/artie-labs/transfer/lib/optimization"

	"github.com/stretchr/testify/assert"
)

func TestTableData_Complete(t *testing.T) {
	db := NewMemoryDB()

	tableName := "table"

	// TableData does not exist
	_, ok := db.TableData()[tableName]
	assert.False(t, ok)

	td := db.GetOrCreateTableData(tableName)
	assert.True(t, td.Empty())
	_, ok = db.TableData()[tableName]
	assert.True(t, ok)

	// Add the td struct
	td.SetTableData(&optimization.TableData{})
	assert.False(t, td.Empty())

	// Wipe via tableData.Wipe()
	td.Wipe()
	assert.True(t, td.Empty())

	// Wipe via ClearTableConfig(...)
	td.SetTableData(&optimization.TableData{})
	assert.False(t, td.Empty())

	db.ClearTableConfig(tableName)
	assert.True(t, td.Empty())
}
