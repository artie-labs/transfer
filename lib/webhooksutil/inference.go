package webhooksutil

import "log/slog"

// GetEventSeverity returns the severity for an event.
func GetEventSeverity(eventType EventType) Severity {
	switch eventType {
	case EventBackFillFailed, ReplicationFailed, UnableToReplicate,
		TableFailed, DedupeFailed, ConnectionFailed, ConfigInvalid:
		return SeverityError
	case TableSkipped, ConnectionLost, ConnectionRetry:
		return SeverityWarning
	case EventBackFillStarted, EventBackFillCompleted, ReplicationStarted,
		TableStarted, TableCompleted, TableEmpty,
		BackfillProgress, DedupeStarted, DedupeCompleted,
		ConnectionEstablished, ConfigValidated:
		return SeverityInfo
	default:
		return SeverityInfo
	}
}

func GetEventCategory(eventType EventType) string {
	switch eventType {
	case EventBackFillStarted, EventBackFillCompleted, EventBackFillFailed, BackfillProgress:
		return "backfill"

	case ReplicationStarted, ReplicationFailed, UnableToReplicate:
		return "replication"

	case TableStarted, TableCompleted, TableFailed, TableSkipped, TableEmpty:
		return "table"

	case DedupeStarted, DedupeCompleted, DedupeFailed:
		return "data_quality"

	case ConnectionEstablished, ConnectionLost, ConnectionRetry, ConnectionFailed:
		return "connection"

	case ConfigValidated, ConfigInvalid:
		return "configuration"

	default:
		return "operation"
	}
}

// GetEventMessage returns a message for an event type
func GetEventMessage(eventType EventType) string {
	switch eventType {
	// backfill events
	case EventBackFillStarted:
		return "Backfill started"
	case EventBackFillCompleted:
		return "Backfill completed"
	case EventBackFillFailed:
		return "Backfill failed"
	case BackfillProgress:
		return "Backfill progress"
	case ReplicationStarted:
		return "Replication started"
	case ReplicationFailed:
		return "Replication failed"
	case UnableToReplicate:
		return "Unable to replicate"

	// table-level events
	case TableStarted:
		return "Table processing started"
	case TableCompleted:
		return "Table processing completed"
	case TableFailed:
		return "Table processing failed"
	case TableSkipped:
		return "Table skipped"
	case TableEmpty:
		return "Table is empty"

	// dedupe & data quality events
	case DedupeStarted:
		return "Deduplication started"
	case DedupeCompleted:
		return "Deduplication completed"
	case DedupeFailed:
		return "Deduplication failed"

	// connection events
	case ConnectionEstablished:
		return "Connection established"
	case ConnectionLost:
		return "Connection lost"
	case ConnectionRetry:
		return "Connection retry"
	case ConnectionFailed:
		return "Connection failed"

	// configuration events
	case ConfigValidated:
		return "Configuration validated"
	case ConfigInvalid:
		return "Configuration invalid"
	default:
		slog.Error("Unknown event type", "eventType", eventType)
		return "Unknown event type"
	}
}
