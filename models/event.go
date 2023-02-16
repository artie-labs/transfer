package models

import (
	"context"
	"github.com/artie-labs/transfer/lib/config/constants"
	"time"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Event struct {
	Table              string
	PrimaryKeyName     string
	PrimaryKeyValue    interface{}
	Data               map[string]interface{} // json serialized column data
	ExecutionTime      time.Time              // When the SQL command was executed
	DropDeletedColumns bool                   // Should we drop deleted columns in the destination?
}

func ToMemoryEvent(ctx context.Context, event cdc.Event, pkName string, pkValue interface{}, tc *kafkalib.TopicConfig) Event {
	return Event{
		Table:              event.Table(),
		PrimaryKeyName:     pkName,
		PrimaryKeyValue:    pkValue,
		ExecutionTime:      event.GetExecutionTime(),
		Data:               event.GetData(ctx, pkName, pkValue, tc),
		DropDeletedColumns: tc.DropDeletedColumns,
	}
}

func (e *Event) IsValid() bool {
	// Does it have a PK or table set?
	if array.Empty([]string{e.Table, e.PrimaryKeyName}) {
		return false
	}

	if e.PrimaryKeyValue == nil {
		return false
	}

	// Check if delete flag exists.
	_, isOk := e.Data[constants.DeleteColumnMarker]
	if !isOk {
		return false
	}

	return true
}
