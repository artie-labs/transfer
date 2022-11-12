package models

import (
	"github.com/artie-labs/transfer/lib/kafkalib"
	"time"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
)

type Event struct {
	Table           string
	PrimaryKeyName  string
	PrimaryKeyValue interface{}
	Data            map[string]interface{} // json serialized column data
	ExecutionTime   time.Time              // When the SQL command was executed
}

func ToMemoryEvent(event cdc.Event, pkName string, pkValue interface{}, tc kafkalib.TopicConfig) Event {
	return Event{
		Table:           event.Table(),
		PrimaryKeyName:  pkName,
		PrimaryKeyValue: pkValue,
		ExecutionTime:   event.GetExecutionTime(),
		Data:            event.GetData(pkName, pkValue, tc),
	}
}

func (e *Event) IsValid() bool {
	// Check if delete flag exists.
	_, isOk := e.Data[config.DeleteColumnMarker]
	if !isOk {
		return false
	}

	return true
}
