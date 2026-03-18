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
	EventBackFillStarted:   {SeverityInfo, "backfill", "Backfill started"},
	EventBackFillCompleted: {SeverityInfo, "backfill", "Backfill completed"},
	EventBackFillFailed:    {SeverityError, "backfill", "Backfill failed"},
	BackfillProgress:       {SeverityInfo, "backfill", "Backfill progress"},
	DedupeStarted:          {SeverityInfo, "backfill", "Deduplication started"},
	DedupeCompleted:        {SeverityInfo, "backfill", "Deduplication completed"},
	DedupeFailed:           {SeverityError, "backfill", "Deduplication failed"},
	// Replication events
	ReplicationStarted: {SeverityInfo, "replication", "Replication started"},
	ReplicationFailed:  {SeverityError, "replication", "Replication failed"},
	RowSkipped:         {SeverityWarning, "replication", "Row skipped"},
	// Connection events
	ConnectionFailed: {SeverityError, "connection", "Connection failed"},
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
