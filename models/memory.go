package models

import (
	"context"
	"sync"

	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
)

const dbKey = "__db"

type TableData struct {
	*optimization.TableData
	sync.RWMutex
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

func (d *DatabaseData) GetTableData(tableName string) (*TableData, bool) {
	d.RLock()
	defer d.RUnlock()
	td, isOk := d.tableData[tableName]
	return td, isOk
}

func (d *DatabaseData) NewTableData(tableName string, td *optimization.TableData) {
	d.Lock()
	defer d.Unlock()

	d.tableData[tableName] = &TableData{
		TableData: td,
	}
	return
}

func (d *DatabaseData) ClearTableConfig(tableName string) {
	d.Lock()
	defer d.Unlock()
	delete(d.tableData, tableName)
	return
}

func (d *DatabaseData) TableData() map[string]*TableData {
	return d.tableData
}
