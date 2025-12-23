package webhooksutil

import "time"

type EventType string
type Severity string

const (
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)
const (
	EventBackFillStarted   EventType = "backfill.started"
	EventBackFillCompleted EventType = "backfill.completed"
	EventBackFillFailed    EventType = "backfill.failed"

	ReplicationStarted EventType = "replication.started"
	ReplicationFailed  EventType = "replication.failed"
	UnableToReplicate  EventType = "unable.to.replicate"
)

// --- TABLE-LEVEL EVENTS  ---

const (
	// TableStarted - Individual table processing started
	TableStarted EventType = "table.started"

	// TableCompleted - Individual table processing completed
	TableCompleted EventType = "table.completed"

	// TableFailed - Individual table processing failed
	TableFailed EventType = "table.failed"

	// TableSkipped - Table intentionally skipped (e.g., filter rules, regex mismatch)
	TableSkipped EventType = "table.skipped"

	// TableEmpty - Table has no data to process
	TableEmpty EventType = "table.empty"
)

// --- PROGRESS EVENTS  ---

const (
	// BackfillProgress - Periodic progress updates during snapshot
	BackfillProgress EventType = "backfill.progress"
)

// --- DEDUPE & DATA QUALITY EVENTS  ---

const (
	// DedupeStarted - Deduplication operation started
	DedupeStarted EventType = "dedupe.started"

	// DedupeCompleted - Deduplication completed
	DedupeCompleted EventType = "dedupe.completed"

	// DedupeFailed - Deduplication failed
	DedupeFailed EventType = "dedupe.failed"
)

// --- CONNECTION EVENTS  ---

const (
	// ConnectionEstablished - Connection to source/destination established
	ConnectionEstablished EventType = "connection.established"

	// ConnectionLost - Connection lost (before retry)
	ConnectionLost EventType = "connection.lost"

	// ConnectionRetry - Attempting to reconnect
	ConnectionRetry EventType = "connection.retry"

	// ConnectionFailed - Connection permanently failed after retries
	ConnectionFailed EventType = "connection.failed"
)

// --- CONFIGURATION EVENTS  ---

const (
	// ConfigValidated - Configuration validated successfully
	ConfigValidated EventType = "config.validated"

	// ConfigInvalid - Configuration validation failed
	ConfigInvalid EventType = "config.invalid"
)

// ============================================================================
// HELPER TYPES FOR COMMON EVENT PROPERTIES
// ============================================================================

// ProgressProperties contains common fields for progress events
type ProgressProperties struct {
	RowsWritten         int64         `json:"rows_written"`
	Duration            time.Duration `json:"duration"`
	EstimatedCompletion *time.Time    `json:"estimated_completion,omitempty"`
	ThroughputPerSecond float64       `json:"throughput_per_second,omitempty"`
}

// TableProperties contains common fields for table events
type TableProperties struct {
	Table    string `json:"table"`
	Schema   string `json:"schema,omitempty"`
	Database string `json:"database,omitempty"`
	RowCount int64  `json:"row_count,omitempty"`
}

// ConnectionProperties contains fields for connection events
type ConnectionProperties struct {
	Host            string        `json:"host,omitempty"`
	Port            int           `json:"port,omitempty"`
	DatabaseType    string        `json:"database_type"` // postgres, mysql, mssql, mongodb, etc.
	RetryCount      int           `json:"retry_count,omitempty"`
	BackoffDuration time.Duration `json:"backoff_duration,omitempty"`
	MaxRetries      int           `json:"max_retries,omitempty"`
}

// ErrorProperties contains fields for error events
type ErrorProperties struct {
	Error             string `json:"error"`
	StackTrace        string `json:"stack_trace,omitempty"`
	RetryCount        int    `json:"retry_count,omitempty"`
	ConsecutiveErrors int    `json:"consecutive_errors,omitempty"`
	Fatal             bool   `json:"fatal,omitempty"`
}

var AllReadyEventTypes = []EventType{
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
