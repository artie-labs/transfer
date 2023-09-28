package models

import (
	"context"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
)

const dbKey = "__db"

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

func (t *TableData) ShouldSkipMerge(cooldown time.Duration) bool {
	padding := 100 * time.Millisecond
	// Padding is added since we are using the flush time interval as the cooldown
	return time.Since(t.lastMergeTime) < (cooldown - padding)
}

func (t *TableData) Empty() bool {
	return t.TableData == nil
}

func (t *TableData) SetTableData(td *optimization.TableData) {
	t.TableData = td
	return
}

type DatabaseData struct {
	tableData map[string]*TableData
	sync.RWMutex
}

func LoadMemoryDB(ctx context.Context) context.Context {
	tableData := make(map[string]*TableData)
	return context.WithValue(ctx, dbKey, &DatabaseData{
		tableData: tableData,
	})
}

func GetMemoryDB(ctx context.Context) *DatabaseData {
	dbValue := ctx.Value(dbKey)
	if dbValue == nil {
		logger.FromContext(ctx).Fatalf("failed to retrieve database from context")
	}

	db, isOk := dbValue.(*DatabaseData)
	if !isOk {
		logger.FromContext(ctx).Fatalf("database data is not the right type *DatabaseData")
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
	return
}

func (d *DatabaseData) TableData() map[string]*TableData {
	return d.tableData
}
