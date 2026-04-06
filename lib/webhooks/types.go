package webhooks

import (
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/redact"
)

type EventType string

const (
	EventBackfillStarted   EventType = "backfill.started"
	EventBackfillCompleted EventType = "backfill.completed"
	EventBackfillFailed    EventType = "backfill.failed"
	EventBackfillProgress  EventType = "backfill.progress"
	EventDedupeStarted     EventType = "dedupe.started"
	EventDedupeCompleted   EventType = "dedupe.completed"
	EventDedupeFailed      EventType = "dedupe.failed"

	EventReplicationStarted EventType = "replication.started"
	EventReplicationFailed  EventType = "replication.failed"
	EventConnectionFailed   EventType = "connection.failed"
	EventRowSkipped         EventType = "row.skipped"

	// Source specific events
	EventDDLSeen EventType = "ddl.seen"

	// Dashboard specific events
	EventDEKGenerated EventType = "dek.generated"
)

// AllEventTypes contains all defined event types.
// Add new event types here when you define them above.
var AllEventTypes = []EventType{
	EventBackfillStarted,
	EventBackfillCompleted,
	EventBackfillFailed,
	EventBackfillProgress,
	EventDedupeStarted,
	EventDedupeCompleted,
	EventDedupeFailed,
	EventReplicationStarted,
	EventReplicationFailed,
	EventConnectionFailed,
	EventRowSkipped,
	EventDDLSeen,
	EventDEKGenerated,
}

type Severity string

const (
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

// Enum implements https://pkg.go.dev/github.com/swaggest/jsonschema-go#Enum
func (s Severity) Enum() []any {
	return []any{
		SeverityInfo,
		SeverityWarning,
		SeverityError,
	}
}

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
	// Source specific events
	EventDDLSeen: {SeverityInfo, "ddl", "DDL seen"},
	// Dashboard specific events:
	EventDEKGenerated: {SeverityInfo, "dashboard", "Data Encryption Key (DEK) generated"},
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

// Service identifies which Artie service emitted the event.
type Service string

const (
	Transfer Service = "transfer"
	Reader   Service = "reader"
	Debezium Service = "debezium"
)

// WebhooksEvent is sent by transfer/reader to the events API.
// The events API unfurls Properties into a flat top-level message in Redis.
type WebhooksEvent struct {
	Event      string            `json:"event"`
	Timestamp  time.Time         `json:"timestamp"`
	MessageID  string            `json:"messageId,omitempty"`
	Properties WebhookProperties `json:"properties"`
}

// [WebhookProperties] is the source of truth for all webhook event fields.
// In transfer/reader: marshaled as the "properties" field of WebhooksEvent.
// In dashboard: embedded at the top level of WebhookEvent (matching the flat
// Redis message after unfurling).
type WebhookProperties struct {
	// Config-level properties (set when client is initialized):
	CompanyUUID      string  `json:"company_uuid"`
	PipelineUUID     string  `json:"pipeline_uuid,omitempty"`
	SourceReaderUUID string  `json:"source_reader_uuid,omitempty"`
	Source           string  `json:"source,omitempty"`      // connector source type, e.g. "postgresql"
	Destination      string  `json:"destination,omitempty"` // connector destination type, e.g. "bigquery"
	Service          Service `json:"service"`               // Artie service: transfer/reader/debezium
	Version          string  `json:"version,omitempty"`     // service version (e.g. "v1.0.0")
	Mode             string  `json:"mode,omitempty"`        // transfer run mode (replication/history)

	// Event-specific properties:
	EventProperties

	// Deprecated - include full error string in Error field instead
	Details string `json:"details,omitempty"`
}

type EventProperties struct {
	Error           string         `json:"error,omitempty"`
	Table           string         `json:"table,omitempty"`
	Schema          string         `json:"schema,omitempty"`
	Database        string         `json:"database,omitempty"`
	Topic           string         `json:"topic,omitempty"`
	RowsWritten     int64          `json:"rows_written,omitempty"`
	DurationSeconds float64        `json:"duration_seconds,omitempty"`
	Reason          string         `json:"reason,omitempty"`
	PrimaryKeys     map[string]any `json:"primary_keys,omitempty"`

	// DDL related properties:
	Query string `json:"query,omitempty"`
	// DDLEvent contains the parsed ANTLR events from the DDL query.
	DDLEvent []map[string]any `json:"ddl_event,omitempty"`

	// DEK related properties:
	EncryptionKeyUUID string `json:"encryption_key_uuid,omitempty"`
	EncryptionKeyName string `json:"encryption_key_name,omitempty"`
	AWSKMSKeyARN      string `json:"aws_kms_key_arn,omitempty"`
}

// Scrub returns a copy with sensitive string fields redacted.
func (e EventProperties) Scrub() EventProperties {
	e.Error = redact.ScrubString(e.Error)
	e.Database = redact.ScrubString(e.Database)
	e.Table = redact.ScrubString(e.Table)
	e.Schema = redact.ScrubString(e.Schema)
	e.Topic = redact.ScrubString(e.Topic)
	e.Reason = redact.ScrubString(e.Reason)
	return e
}
