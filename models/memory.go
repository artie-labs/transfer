package models

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

type databaseData struct {
	TableData map[string]*optimization.TableData
	sync.Mutex
}

type Event struct {
	Table           string
	PrimaryKeyName  string
	PrimaryKeyValue interface{}
	Data            map[string]interface{} // json serialized column data
	ExecutionTime   time.Time              // When the SQL command was executed
}

func ToMemoryEvent(event lib.Event, pkName string, pkValue interface{}, topicConfig kafkalib.TopicConfig) Event {
	evt := Event{
		Table:           event.Source.Table,
		PrimaryKeyName:  pkName,
		PrimaryKeyValue: pkValue,
		ExecutionTime:   event.Source.GetExecutionTime(),
	}

	if len(event.After) == 0 {

		// This is a delete event, so mark it as deleted.
		evt.Data = map[string]interface{}{
			config.DeleteColumnMarker: true,
			evt.PrimaryKeyName:        evt.PrimaryKeyValue,
			topicConfig.IdempotentKey: evt.ExecutionTime.Format(time.RFC3339),
		}
	} else {
		evt.Data = event.After
		evt.Data[config.DeleteColumnMarker] = false
	}

	return evt
}

var InMemoryDB *databaseData

func GetTableConfig(tableName string) map[string]map[string]interface{} {
	return InMemoryDB.TableData[tableName].RowsData
}

func (d *databaseData) ClearTableConfig(tableName string) {
	// WARNING: before you call this, LOCK the table.
	delete(d.TableData, tableName)
}

func (e *Event) IsValid() bool {
	// Check if delete flag exists.
	_, isOk := e.Data[config.DeleteColumnMarker]
	if !isOk {
		return false
	}

	return true
}

func (e *Event) Save(topicConfig *kafkalib.TopicConfig, partition int32, offset string) error {
	if topicConfig == nil {
		return errors.New("topicConfig is missing")
	}

	InMemoryDB.Lock()
	defer InMemoryDB.Unlock()

	if !e.IsValid() {
		return errors.New("event not valid")
	}

	// Does the table exist?
	_, isOk := InMemoryDB.TableData[e.Table]
	if !isOk {
		InMemoryDB.TableData[e.Table] = &optimization.TableData{
			RowsData:           map[string]map[string]interface{}{},
			Columns:            map[string]typing.Kind{},
			PrimaryKey:         e.PrimaryKeyName,
			TopicConfig:        *topicConfig,
			PartitionsToOffset: map[int32]string{},
		}
	}

	// Update the key, offset and TS
	InMemoryDB.TableData[e.Table].RowsData[fmt.Sprint(e.PrimaryKeyValue)] = e.Data
	InMemoryDB.TableData[e.Table].PartitionsToOffset[partition] = offset
	InMemoryDB.TableData[e.Table].LatestCDCTs = e.ExecutionTime

	// Update col if necessary
	for col, val := range e.Data {
		colType, isOk := InMemoryDB.TableData[e.Table].Columns[col]
		if !isOk {
			InMemoryDB.TableData[e.Table].Columns[col] = typing.ParseValue(val)
		} else {
			if colType == typing.Invalid {
				// If colType is Invalid, let's see if we can update it to a better type
				// If everything is nil, we don't need to add a column
				// However, it's important to create a column even if it's nil.
				// This is because we don't want to think that it's okay to drop a column in DWH
				if kind := typing.ParseValue(val); kind != typing.Invalid {
					InMemoryDB.TableData[e.Table].Columns[col] = kind
				}
			}
		}

	}

	return nil
}

func InitMemoryDB() {
	InMemoryDB = &databaseData{
		TableData: map[string]*optimization.TableData{},
	}
}
