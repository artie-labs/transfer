package models

import (
	"context"
	"errors"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
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
func (e *Event) Save(ctx context.Context, topicConfig *kafkalib.TopicConfig, message artie.Message) (bool, error) {
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
			RowsData:                map[string]map[string]interface{}{},
			InMemoryColumns:         map[string]typing.KindDetails{},
			PrimaryKeys:             e.PrimaryKeys(),
			TopicConfig:             *topicConfig,
			PartitionsToLastMessage: map[string][]artie.Message{},
		}
	}

	// Update col if necessary
	sanitizedData := make(map[string]interface{})
	for _col, val := range e.Data {
		// columns need to all be normalized and lower cased.
		newColName := strings.ToLower(_col)

		// Columns here could contain spaces. Every destination treats spaces in a column differently.
		// So far, Snowflake accepts them when escaped properly, however BigQuery does not accept it.
		// Instead of making this more complicated for future destinations, we will escape the spaces by having double underscore `__`
		// So, if customers want to retrieve spaces again, they can replace `__`.
		var containsSpace bool
		containsSpace, newColName = stringutil.EscapeSpaces(newColName)
		if containsSpace {
			// Write the message back if the column has changed.
			sanitizedData[newColName] = val
		}

		if val == "__debezium_unavailable_value" {
			// This is an edge case within Postgres & ORCL
			// TL;DR - Sometimes a column that is unchanged within a DML will not be emitted
			// DBZ has stubbed it out by providing this value, so we will skip it when we see it.
			// See: https://issues.redhat.com/browse/DBZ-4276
			// We are directly adding this column to our in-memory database
			// This ensures that this column exists, we just have an invalid value (so we will not replicate over).
			// However, this will ensure that we do not drop the column within the destination
			inMemoryDB.TableData[e.Table].InMemoryColumns[newColName] = typing.Invalid
			continue
		}

		colTypeDetails, isOk := inMemoryDB.TableData[e.Table].InMemoryColumns[newColName]
		if !isOk {
			inMemoryDB.TableData[e.Table].InMemoryColumns[newColName] = typing.ParseValue(val)
		} else {
			if colTypeDetails.Kind == typing.Invalid.Kind {
				// If colType is Invalid, let's see if we can update it to a better type
				// If everything is nil, we don't need to add a column
				// However, it's important to create a column even if it's nil.
				// This is because we don't want to think that it's okay to drop a column in DWH
				if kindDetails := typing.ParseValue(val); kindDetails.Kind != typing.Invalid.Kind {
					inMemoryDB.TableData[e.Table].InMemoryColumns[newColName] = kindDetails
				}
			}
		}

		sanitizedData[newColName] = val
	}

	// Swap out sanitizedData <> data.
	e.Data = sanitizedData
	inMemoryDB.TableData[e.Table].RowsData[e.PrimaryKeyValue()] = e.Data
	// If the message is Kafka, then we only need the latest one
	// If it's pubsub, we will store all of them in memory. This is because GCP pub/sub REQUIRES us to ack every single message
	if message.Kind() == artie.Kafka {
		inMemoryDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()] = []artie.Message{message}
	} else {
		inMemoryDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()] = append(inMemoryDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()], message)
	}

	inMemoryDB.TableData[e.Table].LatestCDCTs = e.ExecutionTime

	// Increment row count
	inMemoryDB.TableData[e.Table].Rows += 1

	settings := config.FromContext(ctx)
	return inMemoryDB.TableData[e.Table].Rows > settings.Config.BufferRows, nil
}

func LoadMemoryDB() {
	inMemoryDB = &DatabaseData{
		TableData: map[string]*optimization.TableData{},
	}
}
