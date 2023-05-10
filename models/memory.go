package models

import (
	"context"
	"sync"

	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
)

const dbKey = "__db"

type DatabaseData struct {
	TableData map[string]*optimization.TableData
	sync.Mutex
}

func LoadMemoryDB(ctx context.Context) context.Context {
	return context.WithValue(ctx, dbKey, &DatabaseData{
		TableData: map[string]*optimization.TableData{},
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

func (d *DatabaseData) ClearTableConfig(tableName string) {
	// WARNING: before you call this, LOCK the table.
	delete(d.TableData, tableName)
}
