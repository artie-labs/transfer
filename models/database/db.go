package database

import (
	"context"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
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

func (d *Database) NewTable(name string, primaryKeys []string, topicConfig kafkalib.TopicConfig) *optimization.TableData {
	d.Lock()
	defer d.Unlock()

	d.tableData[name] = &optimization.TableData{
		RowsData:                map[string]map[string]interface{}{},
		InMemoryColumns:         map[string]typing.KindDetails{},
		PrimaryKeys:             primaryKeys,
		TopicConfig:             topicConfig,
		PartitionsToLastMessage: map[string][]artie.Message{},
	}

	return d.tableData[name]
}
