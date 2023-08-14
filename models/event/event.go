package event

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/typing/columns"

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
	OptionalSchema map[string]typing.KindDetails
	Columns        *columns.Columns
	ExecutionTime  time.Time // When the SQL command was executed
	Deleted        bool
}

func ToMemoryEvent(ctx context.Context, event cdc.Event, pkMap map[string]interface{}, tc *kafkalib.TopicConfig) Event {
	cols := event.GetColumns(ctx)
	// Now iterate over pkMap and tag each column that is a primary key
	if cols != nil {
		for primaryKey := range pkMap {
			cols.UpsertColumn(primaryKey, columns.UpsertColumnArg{
				PrimaryKey: ptr.ToBool(true),
			})
		}
	}

	return Event{
		Table:          stringutil.Override(event.GetTableName(), tc.TableName),
		PrimaryKeyMap:  pkMap,
		ExecutionTime:  event.GetExecutionTime(),
		OptionalSchema: event.GetOptionalSchema(ctx),
		Columns:        cols,
		Data:           event.GetData(ctx, pkMap, tc),
		Deleted:        event.DeletePayload(),
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
//   - Whether to flush immediately or not
//   - Error
func (e *Event) Save(ctx context.Context, topicConfig *kafkalib.TopicConfig, message artie.Message) (bool, error) {
	if topicConfig == nil {
		return false, errors.New("topicConfig is missing")
	}

	if !e.IsValid() {
		return false, errors.New("event not valid")
	}

	inMemDB := models.GetMemoryDB(ctx)
	// Does the table exist?
	td := inMemDB.GetOrCreateTableData(e.Table)
	td.Lock()
	defer td.Unlock()
	if td.Empty() {
		cols := &columns.Columns{}
		if e.Columns != nil {
			cols = e.Columns
		}

		td.SetTableData(optimization.NewTableData(cols, e.PrimaryKeys(), *topicConfig, e.Table))
	} else {
		if e.Columns != nil {
			// Iterate over this again just in case.
			for _, col := range e.Columns.GetColumns() {
				td.AddInMemoryCol(col)
			}
		}
	}

	// Table columns
	inMemoryColumns := td.ReadOnlyInMemoryCols()
	// Update col if necessary
	sanitizedData := make(map[string]interface{})
	for _col, val := range e.Data {
		// TODO: Refactor this to call columns.EscapeName(...)

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

		if val == constants.ToastUnavailableValuePlaceholder {
			inMemoryColumns.UpsertColumn(newColName, columns.UpsertColumnArg{
				ToastCol: ptr.ToBool(true),
			})
		} else {
			retrievedColumn, isOk := inMemoryColumns.GetColumn(newColName)
			if !isOk {
				// This would only happen if the columns did not get passed in initially.
				fmt.Println("new column", newColName, "typing.ParseValue", typing.ParseValue(ctx, _col, e.OptionalSchema, val), "val", val)
				inMemoryColumns.AddColumn(columns.NewColumn(newColName, typing.ParseValue(ctx, _col, e.OptionalSchema, val)))
			} else {
				if retrievedColumn.KindDetails == typing.Invalid {
					// If colType is Invalid, let's see if we can update it to a better type
					// If everything is nil, we don't need to add a column
					// However, it's important to create a column even if it's nil.
					// This is because we don't want to think that it's okay to drop a column in DWH

					fmt.Println("new column 2", newColName, "typing.ParseValue", typing.ParseValue(ctx, _col, e.OptionalSchema, val), "val", val)
					if kindDetails := typing.ParseValue(ctx, _col, e.OptionalSchema, val); kindDetails.Kind != typing.Invalid.Kind {
						retrievedColumn.KindDetails = kindDetails
						inMemoryColumns.UpdateColumn(retrievedColumn)
					}
				}
			}
		}

		sanitizedData[newColName] = val
	}

	// Now we commit the table columns.
	td.SetInMemoryColumns(inMemoryColumns)

	// Swap out sanitizedData <> data.
	e.Data = sanitizedData
	td.InsertRow(e.PrimaryKeyValue(), e.Data, e.Deleted)
	// If the message is Kafka, then we only need the latest one
	// If it's pubsub, we will store all of them in memory. This is because GCP pub/sub REQUIRES us to ack every single message
	if message.Kind() == artie.Kafka {
		td.PartitionsToLastMessage[message.Partition()] = []artie.Message{message}
	} else {
		td.PartitionsToLastMessage[message.Partition()] = append(td.PartitionsToLastMessage[message.Partition()], message)
	}

	td.LatestCDCTs = e.ExecutionTime
	return td.ShouldFlush(ctx), nil
}
