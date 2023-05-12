package event

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/stringutil"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/models"
)

type Event struct {
	Table          string
	PrimaryKeyMap  map[string]interface{}
	Data           map[string]interface{} // json serialized column data
	OptiomalSchema map[string]typing.KindDetails
	Columns        *typing.Columns
	ExecutionTime  time.Time // When the SQL command was executed
}

func ToMemoryEvent(ctx context.Context, event cdc.Event, pkMap map[string]interface{}, tc *kafkalib.TopicConfig) Event {
	return Event{
		Table:          tc.TableName,
		PrimaryKeyMap:  pkMap,
		ExecutionTime:  event.GetExecutionTime(),
		OptiomalSchema: event.GetOptionalSchema(ctx),
		Columns:        event.GetColumns(),
		Data:           event.GetData(ctx, pkMap, tc),
	}
}

func (e *Event) IsValid() bool {
	// Does it have a PK or table set?
	if array.Empty([]string{e.Table}) {
		return false
	}

	if len(e.PrimaryKeyMap) == 0 {
		return false
	}

	if len(e.Data) == 0 {
		return false
	}

	// Check if delete flag exists.
	_, isOk := e.Data[constants.DeleteColumnMarker]
	if !isOk {
		return false
	}

	return true
}

// PrimaryKeys is returned in a sorted manner to be safe.
// We use PrimaryKeyValue() as our internal identifier within our db
// It is critical to make sure `PrimaryKeyValue()` is a deterministic call.
func (e *Event) PrimaryKeys() []string {
	var keys []string
	for key := range e.PrimaryKeyMap {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

// PrimaryKeyValue - as per above, this needs to return a deterministic k/v string.
func (e *Event) PrimaryKeyValue() string {
	var key string
	for _, pk := range e.PrimaryKeys() {
		key += fmt.Sprintf("%s=%v", pk, e.PrimaryKeyMap[pk])
	}

	return key
}

// Save will save the event into our in memory event
// It will return:
// 1) Whether to flush immediately or not
// 2) Should we re-process this row?
// 3) Error
func (e *Event) Save(ctx context.Context, topicConfig *kafkalib.TopicConfig, message artie.Message) (bool, bool, error) {
	if topicConfig == nil {
		return false, false, errors.New("topicConfig is missing")
	}

	inMemDB := models.GetMemoryDB(ctx)

	inMemDB.Lock()
	defer inMemDB.Unlock()

	if !e.IsValid() {
		return false, false, errors.New("event not valid")
	}

	// Does the table exist?
	_, isOk := inMemDB.TableData[e.Table]
	if !isOk {
		columns := &typing.Columns{}
		if e.Columns != nil {
			columns = e.Columns
		}

		inMemDB.TableData[e.Table] = optimization.NewTableData(columns, e.PrimaryKeys(), *topicConfig)
	} else {
		if e.Columns != nil {
			// Iterate over this again just in case.
			for _, col := range e.Columns.GetColumns() {
				fmt.Println("Are you getting added here?", col.Name, col.KindDetails.Kind)
				inMemDB.TableData[e.Table].AddInMemoryCol(col)
			}
		}
	}

	// Table columns
	inMemoryColumns := inMemDB.TableData[e.Table].InMemoryColumns()

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
			// Check if the column already exists.
			// Early return to reprocess if the column exists and toastColumn = false.
			// And the column type is not invalid.
			col, isOk := inMemoryColumns.GetColumn(newColName)
			if isOk && col.KindDetails != typing.Invalid && col.ToastColumn == false {
				fmt.Println("early return here.", newColName)
				return true, true, nil
			}

			// This is an edge case within Postgres & ORCL
			// TL;DR - Sometimes a column that is unchanged within a DML will not be emitted
			// DBZ has stubbed it out by providing this value, so we will skip it when we see it.
			// See: https://issues.redhat.com/browse/DBZ-4276
			// We are directly adding this column to our in-memory database
			// This ensures that this column exists, we just have an invalid value (so we will not replicate over).
			// However, this will ensure that we do not drop the column within the destination
			inMemoryColumns.AddColumn(typing.Column{
				Name:        newColName,
				KindDetails: typing.Invalid,
				ToastColumn: true,
			})
			continue
		}

		retrievedColumn, isOk := inMemoryColumns.GetColumn(newColName)
		fmt.Println("newColName", newColName, "retrievedColumnKind", retrievedColumn.KindDetails.Kind, "name", retrievedColumn.Name)
		if !isOk {
			// This would only happen if the columns did not get passed in initially.
			inMemoryColumns.AddColumn(typing.Column{
				Name:        newColName,
				KindDetails: typing.ParseValue(_col, e.OptiomalSchema, val),
			})
		} else {
			if retrievedColumn.KindDetails == typing.Invalid {
				// If colType is Invalid, let's see if we can update it to a better type
				// If everything is nil, we don't need to add a column
				// However, it's important to create a column even if it's nil.
				// This is because we don't want to think that it's okay to drop a column in DWH
				if kindDetails := typing.ParseValue(_col, e.OptiomalSchema, val); kindDetails.Kind != typing.Invalid.Kind {
					if retrievedColumn.ToastColumn {
						fmt.Println("or here?")
						// Now that we are here, this means that we have a row that has a value for this toast column
						// In order to prevent a mismatch, we will now force a flush and a re-process.
						return true, true, nil
					}

					inMemoryColumns.UpdateColumn(typing.Column{
						Name:        newColName,
						KindDetails: kindDetails,
					})
				}
			}
		}

		sanitizedData[newColName] = val
	}

	// Now we commit the table columns.
	inMemDB.TableData[e.Table].SetInMemoryColumns(inMemoryColumns)

	// Swap out sanitizedData <> data.
	e.Data = sanitizedData
	inMemDB.TableData[e.Table].InsertRow(e.PrimaryKeyValue(), e.Data)
	// If the message is Kafka, then we only need the latest one
	// If it's pubsub, we will store all of them in memory. This is because GCP pub/sub REQUIRES us to ack every single message
	if message.Kind() == artie.Kafka {
		inMemDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()] = []artie.Message{message}
	} else {
		inMemDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()] = append(inMemDB.TableData[e.Table].PartitionsToLastMessage[message.Partition()], message)
	}

	inMemDB.TableData[e.Table].LatestCDCTs = e.ExecutionTime
	return inMemDB.TableData[e.Table].ShouldFlush(ctx), false, nil
}
