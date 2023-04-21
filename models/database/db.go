package database

import (
	"context"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"log"
	"sync"
)

type Database struct {
	tableData map[string]*optimization.TableData
	sync.Mutex
}

const databaseKey = "_db"

// TODO build inject

func FromContext(ctx context.Context) *Database {
	dbVal := ctx.Value(databaseKey)
	if dbVal == nil {
		log.Fatalf("failed to grab database from context")
	}

	db, isOk := dbVal.(*Database)
	if !isOk {
		log.Fatalf("database in context is not *Database type")
	}

	return db
}

type TableDataWrapper struct {
	Name string
	*optimization.TableData
}

func (d *Database) GetTables() []*TableDataWrapper {
	d.Lock()
	defer d.Unlock()

	var tables []*TableDataWrapper
	for tableName, table := range d.tableData {
		tables = append(tables, &TableDataWrapper{
			Name:      tableName,
			TableData: table,
		})
	}

	return tables
}

func (d *Database) GetTable(name string) *optimization.TableData {
	d.Lock()
	defer d.Unlock()

	tableData, isOk := d.tableData[name]
	if !isOk {
		return nil
	}

	return tableData
}

func (d *Database) WipeTable(name string) {
	d.Lock()
	defer d.Unlock()

	delete(d.tableData, name)
}

func (d *Database) NewTable(name string, primaryKeys []string, topicConfig *kafkalib.TopicConfig) *optimization.TableData {
	d.Lock()
	defer d.Unlock()

	d.tableData[name] = optimization.NewTableData(primaryKeys, topicConfig)
	return d.tableData[name]
}
