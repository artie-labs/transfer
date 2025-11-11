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

type EventPayload struct {
	Event       string         `json:"event"`
	Timestamp   string         `json:"timestamp"`
	MessageID   string         `json:"messageID"`
	Properties  map[string]any `json:"properties"`
	ExtraFields map[string]any `json:"extraFields"`
}

// EventTrackingEvent implements the [cdc.Event] interface
type EventTrackingEvent struct {
	payload EventPayload
}

func (e *EventTrackingEvent) GetExecutionTime() time.Time {
	t, err := time.Parse(time.RFC3339, e.payload.Timestamp)
	if err != nil {
		// If parsing fails, try RFC3339Nano
		t, err = time.Parse(time.RFC3339Nano, e.payload.Timestamp)
		if err != nil {
			slog.Error("failed to parse timestamp", slog.String("timestamp", e.payload.Timestamp), slog.Any("error", err))
			// Timestamp is required, but if parsing fails, return current time as fallback
			return time.Now().UTC()
		}
	}

	return t.UTC()
}

func (e *EventTrackingEvent) Operation() constants.Operation {
	return constants.Create
}

func (e *EventTrackingEvent) DeletePayload() bool {
	return false
}

func (e *EventTrackingEvent) GetTableName() string {
	return e.payload.Event
}

func (e *EventTrackingEvent) GetFullTableName() string {
	return e.GetTableName()
}

func (e *EventTrackingEvent) GetSourceMetadata() (string, error) {
	return "{}", nil
}

func (e *EventTrackingEvent) GetData(tc kafkalib.TopicConfig) (map[string]any, error) {
	// The table data consists of the properties, additional top-level fields, and the ID & timestamp.
	retMap := make(map[string]any)
	maps.Copy(retMap, e.payload.Properties)
	maps.Copy(retMap, e.payload.ExtraFields)
	retMap["id"] = e.payload.MessageID
	retMap["timestamp"] = e.payload.Timestamp

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

func (e *EventTrackingEvent) GetOptionalSchema() (map[string]typing.KindDetails, error) {
	// Event tracking format doesn't have a schema
	return nil, nil
}

func (e *EventTrackingEvent) GetColumns(reservedColumns map[string]bool) (*columns.Columns, error) {
	var cols columns.Columns
	for k := range e.payload.Properties {
		cols.AddColumn(columns.NewColumn(columns.EscapeName(k, reservedColumns), typing.Invalid))
	}
	for k := range e.payload.ExtraFields {
		cols.AddColumn(columns.NewColumn(columns.EscapeName(k, reservedColumns), typing.Invalid))
	}

	cols.AddColumn(columns.NewColumn(columns.EscapeName("id", reservedColumns), typing.Invalid))
	cols.AddColumn(columns.NewColumn(columns.EscapeName("timestamp", reservedColumns), typing.Invalid))

	return &cols, nil
}
