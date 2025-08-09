package models

import (
	"testing"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/optimization"

	"github.com/stretchr/testify/assert"
)

func TestTableData_Complete(t *testing.T) {
	db := NewMemoryDB()
	tableID := cdc.NewTableID("schema", "table")
	{
		// Does not exist
		_, ok := db.TableData()[tableID]
		assert.False(t, ok)
	}
	{
		// Exists after we created it.
		td := db.GetOrCreateTableData(tableID, "topic")
		assert.True(t, td.Empty())
		_, ok := db.TableData()[tableID]
		assert.True(t, ok)
		assert.Equal(t, "topic", td.topic)

		// Various ways to wipe the database data
		{
			// Add the td struct
			td.SetTableData(&optimization.TableData{})
			assert.False(t, td.Empty())

			// Wipe via tableData.Wipe() and should be empty now.
			td.Wipe()
			assert.True(t, td.Empty())
		}
		{
			// Wipe via ClearTableConfig(...)
			td.SetTableData(&optimization.TableData{})
			assert.False(t, td.Empty())
			db.ClearTableConfig(tableID)
			assert.True(t, td.Empty())
		}
	}
}
