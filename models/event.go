package models

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config/constants"
	"time"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Event struct {
	Table         string
	PrimaryKeyMap map[string]interface{}
	Data          map[string]interface{} // json serialized column data
	ExecutionTime time.Time              // When the SQL command was executed
}

func ToMemoryEvent(ctx context.Context, event cdc.Event, pkMap map[string]interface{}, tc *kafkalib.TopicConfig) Event {
	return Event{
		Table:         tc.TableName,
		PrimaryKeyMap: pkMap,
		ExecutionTime: event.GetExecutionTime(),
		Data:          event.GetData(ctx, pkMap, tc),
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

func (e *Event) PrimaryKeys() []string {
	// TODO - Test
	var keys []string
	for key := range e.PrimaryKeyMap {
		keys = append(keys, key)
	}

	return keys
}

func (e *Event) PrimaryKeyValue() string {
	// TODO - Test
	var key string
	for k, v := range e.PrimaryKeyMap {
		key += fmt.Sprintf("%s=%v", k, v)
	}

	return key
}
