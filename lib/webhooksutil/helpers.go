package webhooksutil

import "log/slog"

// EventMetadata contains all metadata for an event type.
type EventMetadata struct {
	Severity Severity
	Category string
	Message  string
}

var eventMetadataMap = map[EventType]EventMetadata{
	// Backfill events
	EventBackFillStarted:   {SeverityInfo, "backfill", "Backfill started"},
	EventBackFillCompleted: {SeverityInfo, "backfill", "Backfill completed"},
	EventBackFillFailed:    {SeverityError, "backfill", "Backfill failed"},
	BackfillProgress:       {SeverityInfo, "backfill", "Backfill progress"},
	// Replication events
	ReplicationStarted: {SeverityInfo, "replication", "Replication started"},
	ReplicationFailed:  {SeverityError, "replication", "Replication failed"},
	UnableToReplicate:  {SeverityError, "replication", "Unable to replicate"},
	// Table events
	TableStarted:   {SeverityInfo, "table", "Table processing started"},
	TableCompleted: {SeverityInfo, "table", "Table processing completed"},
	TableFailed:    {SeverityError, "table", "Table processing failed"},
	TableSkipped:   {SeverityWarning, "table", "Table skipped"},
	TableEmpty:     {SeverityInfo, "table", "Table is empty"},
	// Dedupe events
	DedupeStarted:   {SeverityInfo, "data_quality", "Deduplication started"},
	DedupeCompleted: {SeverityInfo, "data_quality", "Deduplication completed"},
	DedupeFailed:    {SeverityError, "data_quality", "Deduplication failed"},
	// Connection events
	ConnectionEstablished: {SeverityInfo, "connection", "Connection established"},
	ConnectionLost:        {SeverityWarning, "connection", "Connection lost"},
	ConnectionRetry:       {SeverityWarning, "connection", "Connection retry"},
	ConnectionFailed:      {SeverityError, "connection", "Connection failed"},
	// Configuration events
	ConfigValidated: {SeverityInfo, "configuration", "Configuration validated"},
	ConfigInvalid:   {SeverityError, "configuration", "Configuration invalid"},
}

func GetEventMetadata(eventType EventType) EventMetadata {
	if metadata, ok := eventMetadataMap[eventType]; ok {
		return metadata
	}
	slog.Error("Unknown event type", "eventType", eventType)
	return EventMetadata{SeverityInfo, "operation", "Unknown event type"}
}

func GetEventSeverity(eventType EventType) Severity {
	return GetEventMetadata(eventType).Severity
}

func GetEventCategory(eventType EventType) string {
	return GetEventMetadata(eventType).Category
}

func GetEventMessage(eventType EventType) string {
	return GetEventMetadata(eventType).Message
}
