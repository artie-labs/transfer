package models

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"sort"
	"time"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/kafkalib"
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
