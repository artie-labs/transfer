package models

import (
	"errors"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
	"strings"
	"sync"
)

type DatabaseData struct {
	TableData map[string]*optimization.TableData
	sync.Mutex
}

func GetMemoryDB() *DatabaseData {
	return inMemoryDB
}

// TODO: We should be able to swap out inMemoryDB per flush and make that non-blocking.
var inMemoryDB *DatabaseData

func (d *DatabaseData) ClearTableConfig(tableName string) {
	// WARNING: before you call this, LOCK the table.
	delete(d.TableData, tableName)
}

// Save will save the event into our in memory event
// It will return two values, a boolean and error
// The boolean signifies whether we should flush immediately or not. This is because Snowflake has a constraint
// On the number of elements within an expression.
// The other, error - is returned to see if anything went awry.
func (e *Event) Save(topicConfig *kafkalib.TopicConfig, partition int32, offset string) (bool, error) {
	if topicConfig == nil {
		return false, errors.New("topicConfig is missing")
	}

	inMemoryDB.Lock()
	defer inMemoryDB.Unlock()

	if !e.IsValid() {
		return false, errors.New("event not valid")
	}

	// Does the table exist?
	_, isOk := inMemoryDB.TableData[e.Table]
	if !isOk {
		inMemoryDB.TableData[e.Table] = &optimization.TableData{
			RowsData:           map[string]map[string]interface{}{},
			Columns:            map[string]typing.Kind{},
			PrimaryKey:         e.PrimaryKeyName,
			TopicConfig:        *topicConfig,
			PartitionsToOffset: map[int32]string{},
		}
	}

	// Update the key, offset and TS
	inMemoryDB.TableData[e.Table].RowsData[fmt.Sprint(e.PrimaryKeyValue)] = e.Data
	inMemoryDB.TableData[e.Table].PartitionsToOffset[partition] = offset
	inMemoryDB.TableData[e.Table].LatestCDCTs = e.ExecutionTime

	// Increment row count
	inMemoryDB.TableData[e.Table].Rows += 1

	// Update col if necessary
	for col, val := range e.Data {
		col = strings.ToLower(col)
		colType, isOk := inMemoryDB.TableData[e.Table].Columns[col]
		if !isOk {
			fmt.Println("val", val, "typing", typing.ParseValue(val))
			inMemoryDB.TableData[e.Table].Columns[col] = typing.ParseValue(val)
		} else {
			if colType == typing.Invalid {
				// If colType is Invalid, let's see if we can update it to a better type
				// If everything is nil, we don't need to add a column
				// However, it's important to create a column even if it's nil.
				// This is because we don't want to think that it's okay to drop a column in DWH
				if kind := typing.ParseValue(val); kind != typing.Invalid {
					inMemoryDB.TableData[e.Table].Columns[col] = kind
				}
			}
		}
	}

	return inMemoryDB.TableData[e.Table].Rows > config.SnowflakeArraySize, nil
}

func LoadMemoryDB() {
	inMemoryDB = &DatabaseData{
		TableData: map[string]*optimization.TableData{},
	}
}
