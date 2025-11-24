package eventtracking

import (
	"maps"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

// EventPayload implements the [cdc.Event] interface
type EventPayload struct {
	Event       string         `json:"event"`
	Timestamp   time.Time      `json:"timestamp"`
	MessageID   string         `json:"messageID"`
	Properties  map[string]any `json:"properties"`
	ExtraFields map[string]any `json:"extraFields"`
}

func (e *EventPayload) GetExecutionTime() time.Time {
	return e.Timestamp.UTC()
}

func (e *EventPayload) Operation() constants.Operation {
	return constants.Create
}

func (e *EventPayload) DeletePayload() bool {
	return false
}

func (e *EventPayload) GetTableName() string {
	// This will be used if no table name is specified in the topic config.
	return e.Event
}

func (e *EventPayload) GetFullTableName() string {
	return e.GetTableName()
}

func (e *EventPayload) GetSourceMetadata() (string, error) {
	return "{}", nil
}

func (e *EventPayload) GetData(tc kafkalib.TopicConfig) (map[string]any, error) {
	// The table data consists of the properties, additional top-level fields, and the ID & timestamp.
	retMap := make(map[string]any)
	maps.Copy(retMap, e.Properties)
	maps.Copy(retMap, e.ExtraFields)
	retMap["id"] = e.MessageID
	retMap["timestamp"] = e.Timestamp
	// By default, include the event name as a column; it can be excluded via [ColumnsToExclude] if needed.
	retMap["event"] = e.Event

	// Add Artie-specific metadata columns
	retMap[constants.DeleteColumnMarker] = false
	retMap[constants.OnlySetDeleteColumnMarker] = false
	if tc.IncludeArtieUpdatedAt {
		retMap[constants.UpdateColumnMarker] = time.Now().UTC()
	}
	if tc.IncludeDatabaseUpdatedAt {
		retMap[constants.DatabaseUpdatedColumnMarker] = e.GetExecutionTime().UTC()
	}

	return retMap, nil
}

func (e *EventPayload) GetOptionalSchema() (map[string]typing.KindDetails, error) {
	// Event tracking format doesn't have a schema
	return nil, nil
}

func (e *EventPayload) GetColumns(reservedColumns map[string]bool) (*columns.Columns, error) {
	var cols columns.Columns
	for k := range e.Properties {
		cols.AddColumn(columns.NewColumn(columns.EscapeName(k, reservedColumns), typing.Invalid))
	}
	for k := range e.ExtraFields {
		cols.AddColumn(columns.NewColumn(columns.EscapeName(k, reservedColumns), typing.Invalid))
	}

	cols.AddColumn(columns.NewColumn(columns.EscapeName("id", reservedColumns), typing.Invalid))
	cols.AddColumn(columns.NewColumn(columns.EscapeName("timestamp", reservedColumns), typing.Invalid))

	return &cols, nil
}
