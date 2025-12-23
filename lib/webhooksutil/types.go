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

// --- TABLE-LEVEL EVENTS (5 events) ---

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

// --- PROGRESS EVENTS (1 event) ---

const (
	// BackfillProgress - Periodic progress updates during snapshot
	BackfillProgress EventType = "backfill.progress"
)

// --- DEDUPE & DATA QUALITY EVENTS (3 events) ---

const (
	// DedupeStarted - Deduplication operation started
	DedupeStarted EventType = "dedupe.started"

	// DedupeCompleted - Deduplication completed
	DedupeCompleted EventType = "dedupe.completed"

	// DedupeFailed - Deduplication failed
	DedupeFailed EventType = "dedupe.failed"
)

// --- CONNECTION EVENTS (4 events) ---

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

// --- CONFIGURATION EVENTS (2 events) ---

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

// ============================================================================
// SEVERITY MAPPING HELPERS
// ============================================================================

// ============================================================================
// ALL READY EVENT TYPES (for iteration/validation)
// ============================================================================

// AllReadyEventTypes contains all event types that are ready to use now
// 6 existing + 15 new = 21 total
var AllReadyEventTypes = []EventType{
	// Existing (6)
	EventBackFillStarted,
	EventBackFillCompleted,
	EventBackFillFailed,
	ReplicationStarted,
	ReplicationFailed,
	UnableToReplicate,

	// New - Table (5)
	TableStarted,
	TableCompleted,
	TableFailed,
	TableSkipped,
	TableEmpty,

	// New - Progress (1)
	BackfillProgress,

	// New - Dedupe (3)
	DedupeStarted,
	DedupeCompleted,
	DedupeFailed,

	// New - Connection (4)
	ConnectionEstablished,
	ConnectionLost,
	ConnectionRetry,
	ConnectionFailed,

	// New - Config (2)
	ConfigValidated,
	ConfigInvalid,
}

// ============================================================================
// USAGE EXAMPLES
// ============================================================================

/*
Example usage in reader:

// Table started
webhookClient.SendEvent(ctx, TableStarted, map[string]any{
	"table": "users",
	"schema": "public",
	"database": "production_db",
	"operation_type": "snapshot",
})

// Table completed
webhookClient.SendEvent(ctx, TableCompleted, map[string]any{
	"table": "users",
	"schema": "public",
	"database": "production_db",
	"rows_written": 1500000,
	"duration_seconds": 145.5,
	"throughput_per_second": 10309.3,
})

// Backfill progress (emit every 100k rows)
webhookClient.SendEvent(ctx, BackfillProgress, map[string]any{
	"table": "orders",
	"rows_written": 250000,
	"total_duration_seconds": 45.2,
	"batch_size": 10000,
	"batch_duration_seconds": 1.8,
})

// Dedupe started
webhookClient.SendEvent(ctx, DedupeStarted, map[string]any{
	"table": "customers",
	"schema": "public",
	"database": "production_db",
	"primary_keys": []string{"id"},
	"rows_to_dedupe": 1500000,
})

// Connection established
webhookClient.SendEvent(ctx, ConnectionEstablished, map[string]any{
	"database_type": "postgres",
	"database": "production_db",
})

// Config validated
webhookClient.SendEvent(ctx, ConfigValidated, map[string]any{
	"source": "postgres",
	"destination": "snowflake",
	"mode": "snapshot",
})
*/
