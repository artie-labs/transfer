package webhooks

import "log/slog"

// EventMetadata contains all metadata for an event type.
type EventMetadata struct {
	Severity Severity
	Category string
	Message  string
}

var eventMetadataMap = map[EventType]EventMetadata{
	// Backfill events
	EventBackfillStarted:   {SeverityInfo, "backfill", "Backfill started"},
	EventBackfillCompleted: {SeverityInfo, "backfill", "Backfill completed"},
	EventBackfillFailed:    {SeverityError, "backfill", "Backfill failed"},
	EventBackfillProgress:  {SeverityInfo, "backfill", "Backfill progress"},
	EventDedupeStarted:     {SeverityInfo, "backfill", "Deduplication started"},
	EventDedupeCompleted:   {SeverityInfo, "backfill", "Deduplication completed"},
	EventDedupeFailed:      {SeverityError, "backfill", "Deduplication failed"},
	// Replication events
	EventReplicationStarted: {SeverityInfo, "replication", "Replication started"},
	EventReplicationFailed:  {SeverityError, "replication", "Replication failed"},
	EventRowSkipped:         {SeverityWarning, "replication", "Row skipped"},
	// Connection events
	EventConnectionFailed: {SeverityError, "connection", "Connection failed"},
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
