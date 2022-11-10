package models

import (
	"time"

	"github.com/artie-labs/transfer/lib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

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

func (e *Event) IsValid() bool {
	// Check if delete flag exists.
	_, isOk := e.Data[config.DeleteColumnMarker]
	if !isOk {
		return false
	}

	return true
}
