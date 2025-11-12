package eventtracking

import (
	"log/slog"
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
	Timestamp   string         `json:"timestamp"`
	MessageID   string         `json:"messageID"`
	Properties  map[string]any `json:"properties"`
	ExtraFields map[string]any `json:"extraFields"`
}

func (e *EventPayload) GetExecutionTime() time.Time {
	t, err := time.Parse(time.RFC3339Nano, e.Timestamp)
	if err != nil {
		slog.Error("failed to parse timestamp", slog.String("timestamp", e.Timestamp), slog.Any("error", err))
		// Timestamp is required, but if parsing fails, return current time as fallback
		return time.Now().UTC()
	}

	return t.UTC()
}

func (e *EventPayload) Operation() constants.Operation {
	return constants.Create
}

func (e *EventPayload) DeletePayload() bool {
	return false
}

func (e *EventPayload) GetTableName() string {
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
