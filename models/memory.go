package models

import (
	"context"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"

	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
)

// TableData is a wrapper around *optimization.TableData which stores the actual underlying tableData.
// The wrapper here is just to have a mutex. Any of the ptr methods on *TableData will require callers to use their own locks.
// We did this because certain operations require different locking patterns
type TableData struct {
	*optimization.TableData
	lastMergeTime time.Time
	sync.Mutex
}

func (t *TableData) Wipe() {
	t.TableData = nil
	t.lastMergeTime = time.Now()
}

// ShouldSkipMerge - this function is only used when the flush reason was time-based.
// We want to add this in so that it can strike a balance between the Flush and Consumer go-routines on when to merge.
// Say our flush interval is 5 mins and it flushed 4 mins ago based on size or rows - we don't want to flush right after since the buffer would be mostly empty.
func (t *TableData) ShouldSkipMerge(cooldown time.Duration) bool {
	if cooldown > 1*time.Minute {
		confidenceInterval := 0.25
		confidenceDuration := time.Duration(confidenceInterval * float64(cooldown))

		// Subtract the confidenceDuration from the cooldown to get the adjusted cooldown
		cooldown = cooldown - confidenceDuration
	}

	return time.Since(t.lastMergeTime) < cooldown
}

func (t *TableData) Empty() bool {
	return t.TableData == nil
}

func (t *TableData) SetTableData(td *optimization.TableData) {
	t.TableData = td
}

type DatabaseData struct {
	tableData map[string]*TableData
	sync.RWMutex
}

func LoadMemoryDB(ctx context.Context) context.Context {
	tableData := make(map[string]*TableData)
	return context.WithValue(ctx, constants.DatabaseKey, &DatabaseData{
		tableData: tableData,
	})
}

func GetMemoryDB(ctx context.Context) *DatabaseData {
	dbValue := ctx.Value(constants.DatabaseKey)
	if dbValue == nil {
		logger.Fatal("failed to retrieve database from context")
	}

	db, isOk := dbValue.(*DatabaseData)
	if !isOk {
		logger.Fatal("database data is not the right type *DatabaseData")
	}

	return db
}

func (d *DatabaseData) GetOrCreateTableData(tableName string) *TableData {
	d.Lock()
	defer d.Unlock()

	table, exists := d.tableData[tableName]
	if !exists {
		table = &TableData{
			Mutex: sync.Mutex{},
		}
		d.tableData[tableName] = table
	}

	return table
}

func (d *DatabaseData) ClearTableConfig(tableName string) {
	d.Lock()
	defer d.Unlock()
	d.tableData[tableName].Wipe()
}

func (d *DatabaseData) TableData() map[string]*TableData {
	return d.tableData
}
