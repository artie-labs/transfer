package models

import (
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/optimization"
)

// TableData is a wrapper around *optimization.TableData which stores the actual underlying tableData.
// The wrapper here is just to have a mutex. Any of the ptr methods on *TableData will require callers to use their own locks.
// We did this because certain operations require different locking patterns
type TableData struct {
	*optimization.TableData
	lastFlushTime time.Time
	sync.Mutex
}

func (t *TableData) Wipe() {
	t.TableData = nil
	t.lastFlushTime = time.Now()
}

// ShouldSkipFlush - this function is only used when the flush reason was time-based.
// We want to add this in so that it can strike a balance between the Flush and Consumer go-routines on when to merge.
// Say our flush interval is 5 min, and it flushed 4 min ago based on size or rows - we don't want to flush right after since the buffer would be mostly empty.
func (t *TableData) ShouldSkipFlush(cooldown time.Duration) bool {
	if cooldown > 1*time.Minute {
		confidenceInterval := 0.25
		confidenceDuration := time.Duration(confidenceInterval * float64(cooldown))

		// Subtract the confidenceDuration from the cooldown to get the adjusted cooldown
		cooldown = cooldown - confidenceDuration
	}

	return time.Since(t.lastFlushTime) < cooldown
}

func (t *TableData) Empty() bool {
	return t.TableData == nil
}

func (t *TableData) SetTableData(td *optimization.TableData) {
	t.TableData = td
}

type DatabaseData struct {
	tableData map[cdc.TableID]*TableData
	sync.RWMutex
}

func NewMemoryDB() *DatabaseData {
	tableData := make(map[cdc.TableID]*TableData)
	return &DatabaseData{
		tableData: tableData,
	}
}

func (d *DatabaseData) GetOrCreateTableData(tableID cdc.TableID) *TableData {
	d.Lock()
	defer d.Unlock()

	table, exists := d.tableData[tableID]
	if !exists {
		table = &TableData{
			Mutex: sync.Mutex{},
		}
		d.tableData[tableID] = table
	}

	return table
}

func (d *DatabaseData) ClearTableConfig(tableID cdc.TableID) {
	d.Lock()
	defer d.Unlock()
	d.tableData[tableID].Wipe()
}

func (d *DatabaseData) TableData() map[cdc.TableID]*TableData {
	return d.tableData
}
