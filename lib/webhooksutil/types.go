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

// AllEventTypes contains all defined event types
// Add new event types here when you define them above
var AllEventTypes = []EventType{
	EventBackFillStarted,
	EventBackFillCompleted,
	EventBackFillFailed,
	ReplicationStarted,
	ReplicationFailed,
	UnableToReplicate,
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

type Source string

const (
	Transfer  Source = "transfer"
	Reader    Source = "reader"
	Debezium  Source = "debezium"
	EventsAPI Source = "eventsAPI"
)

type WebhooksEvent struct {
	Event      string         `json:"event"`
	Timestamp  time.Time      `json:"timestamp"`
	Properties map[string]any `json:"properties"`
}

type ProgressProperties struct {
	RowsWritten         int64         `json:"rowsWritten"`
	Duration            time.Duration `json:"duration"`
	EstimatedCompletion *time.Time    `json:"estimatedCompletion,omitempty"`
	ThroughputPerSecond float64       `json:"throughputPerSecond,omitempty"`
}

type TableProperties struct {
	Table    string `json:"table"`
	Schema   string `json:"schema,omitempty"`
	Database string `json:"database,omitempty"`
	RowCount int64  `json:"rowCount,omitempty"`
}

type ConnectionProperties struct {
	Host            string        `json:"host,omitempty"`
	Port            int           `json:"port,omitempty"`
	DatabaseType    string        `json:"databaseType"` // postgres, mysql, mssql, mongodb, etc.
	RetryCount      int           `json:"retryCount,omitempty"`
	BackoffDuration time.Duration `json:"backoffDuration,omitempty"`
	MaxRetries      int           `json:"maxRetries,omitempty"`
}

type ErrorProperties struct {
	Error             string `json:"error"`
	StackTrace        string `json:"stackTrace,omitempty"`
	RetryCount        int    `json:"retryCount,omitempty"`
	ConsecutiveErrors int    `json:"consecutiveErrors,omitempty"`
	Fatal             bool   `json:"fatal,omitempty"`
}
