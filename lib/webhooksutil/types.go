package webhooksutil

import "time"

type EventType string

const (
	EventBackFillStarted   EventType = "backfill.started"
	EventBackFillCompleted EventType = "backfill.completed"
	EventBackFillFailed    EventType = "backfill.failed"

	ReplicationStarted EventType = "replication.started"
	ReplicationFailed  EventType = "replication.failed"
	UnableToReplicate  EventType = "unable.to.replicate"
	RowSkipped         EventType = "row.skipped"
)

const (
	TableStarted   EventType = "table.started"
	TableCompleted EventType = "table.completed"
	TableFailed    EventType = "table.failed"
	TableSkipped   EventType = "table.skipped"
	TableEmpty     EventType = "table.empty"
)

const (
	BackfillProgress EventType = "backfill.progress"
)

const (
	DedupeStarted   EventType = "dedupe.started"
	DedupeCompleted EventType = "dedupe.completed"
	DedupeFailed    EventType = "dedupe.failed"
)

const (
	ConnectionEstablished EventType = "connection.established"
	ConnectionLost        EventType = "connection.lost"
	ConnectionRetry       EventType = "connection.retry"
	ConnectionFailed      EventType = "connection.failed"
)

const (
	ConfigValidated EventType = "config.validated"
	ConfigInvalid   EventType = "config.invalid"
)

// AllEventTypes contains all defined event types.
// Add new event types here when you define them above.
var AllEventTypes = []EventType{
	EventBackFillStarted,
	EventBackFillCompleted,
	EventBackFillFailed,
	ReplicationStarted,
	ReplicationFailed,
	UnableToReplicate,
	RowSkipped,
	TableStarted,
	TableCompleted,
	TableFailed,
	TableSkipped,
	TableEmpty,
	BackfillProgress,
	DedupeStarted,
	DedupeCompleted,
	DedupeFailed,
	ConnectionEstablished,
	ConnectionLost,
	ConnectionRetry,
	ConnectionFailed,
	ConfigValidated,
	ConfigInvalid,
}

type Severity string

const (
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

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

// WebhookProperties is the source of truth for all webhook event fields.
// In transfer/reader: marshaled as the "properties" field of WebhooksEvent.
// In dashboard: embedded at the top level of WebhookEvent (matching the flat
// Redis message after unfurling).
type WebhookProperties struct {
	// Config-level fields (set from WebhookSettings at client init)
	CompanyUUID      string `json:"company_uuid"`
	PipelineUUID     string `json:"pipeline_uuid,omitempty"`
	SourceReaderUUID string `json:"source_reader_uuid,omitempty"`
	Source           string `json:"source,omitempty"`      // connector source type, e.g. "postgresql"
	Destination      string `json:"destination,omitempty"` // connector destination type, e.g. "bigquery"

	// Set by BuildProperties
	Service Service `json:"service"` // Artie service: transfer/reader/debezium

	// Auto-set at client init (not passed per-event)
	Mode    string `json:"mode,omitempty"`    // transfer run mode (e.g. "replication"); from WebhookSettings
	Version string `json:"version,omitempty"` // binary version; passed to NewWebhooksClient at startup

	Error           string   `json:"error,omitempty"`
	Table           string   `json:"table,omitempty"`
	Schema          string   `json:"schema,omitempty"`
	Database        string   `json:"database,omitempty"`
	Topic           string   `json:"topic,omitempty"`
	RowsWritten     int64    `json:"rows_written,omitempty"`
	DurationSeconds float64  `json:"duration_seconds,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	PrimaryKeys     []string `json:"primary_keys,omitempty"`

	// Deprecated - include full error string in Error field instead
	Details string `json:"details,omitempty"`
}

// SendEventArgs is passed by call sites to SendEvent.
// The client fills in config-level and metadata fields automatically.
type SendEventArgs struct {
	Error           string
	Table           string
	Schema          string
	Database        string
	Topic           string
	RowsWritten     int64
	DurationSeconds float64
	Reason          string
	PrimaryKeys     []string
}
