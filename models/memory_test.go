package models

import (
	"context"

	"github.com/artie-labs/transfer/lib/optimization"

	"github.com/stretchr/testify/assert"
)

func (m *ModelsTestSuite) TestTableData_Complete() {
	ctx := context.Background()
	ctx = LoadMemoryDB(ctx)

	db := GetMemoryDB(ctx)
	tableName := "table"

	// TableData does not exist
	_, isOk := db.TableData()[tableName]
	assert.False(m.T(), isOk)

	td := db.GetOrCreateTableData(tableName)
	assert.True(m.T(), td.Empty())
	_, isOk = db.TableData()[tableName]
	assert.True(m.T(), isOk)

	// Add the td struct
	td.SetTableData(&optimization.TableData{})
	assert.False(m.T(), td.Empty())

	// Wipe via tableData.Wipe()
	td.Wipe()
	assert.True(m.T(), td.Empty())

	// Wipe via ClearTableConfig(...)
	td.SetTableData(&optimization.TableData{})
	assert.False(m.T(), td.Empty())

	db.ClearTableConfig(tableName)
	assert.True(m.T(), td.Empty())
}
