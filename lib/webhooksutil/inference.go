package webhooksutil

import "log/slog"

// BuildSeverity returns the appropriate severity level for a given event type.
func BuildSeverity(eventType EventType) Severity {
	switch eventType {
	case EventBackFillStarted, EventBackFillCompleted, ReplicationStarted:
		return SeverityInfo
	case ReplicationFailed, UnableToReplicate, EventBackFillFailed:
		return SeverityError
	}
	return SeverityInfo
}

// BuildMessage returns a message for a given event type.
func BuildMessage(eventType EventType) string {
	switch eventType {
	case EventBackFillStarted:
		return "Backfill started"
	case EventBackFillCompleted:
		return "Backfill completed"
	case ReplicationStarted:
		return "Replication started"
	case UnableToReplicate:
		return "Unable to replicate"
	case EventBackFillFailed:
		return "Backfill failed"
	case ReplicationFailed:
		return "Replication failed"
	default:
		slog.Error("Unknown event type", "eventType", eventType)
		return "Unknown event type"
	}
}
