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

type Event struct {
	PipelineID string         `json:"pipeline_id"`
	EventType  EventType      `json:"event_type"`
	Message    string         `json:"message"`
	Source     Source         `json:"source"`
	Timestamp  time.Time      `json:"timestamp"`
	Context    map[string]any `json:"context"`
	Severity   Severity       `json:"severity"`
	PodID      string         `json:"pod_id"`
	TableID    []string       `json:"table_id,omitempty"`
}
