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

// AllEventTypes contains all defined event types
// Add new event types here when you define them above
var AllEventTypes = []EventType{
	EventBackFillStarted,
	EventBackFillCompleted,
	EventBackFillFailed,
	ReplicationStarted,
	ReplicationFailed,
	UnableToReplicate,
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
