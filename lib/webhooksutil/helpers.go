package webhooksutil

import "context"

// GetEventSeverity returns the appropriate severity for an event type
func GetEventSeverity(eventType EventType) Severity {
	switch eventType {
	// Error severity
	case EventBackFillFailed, ReplicationFailed, UnableToReplicate,
		TableFailed, DedupeFailed, ConnectionFailed, ConfigInvalid:
		return SeverityError

	// Warning severity
	case TableSkipped, ConnectionLost, ConnectionRetry:
		return SeverityWarning

	// Info severity (default)
	case EventBackFillStarted, EventBackFillCompleted, ReplicationStarted,
		TableStarted, TableCompleted, TableEmpty,
		BackfillProgress, DedupeStarted, DedupeCompleted,
		ConnectionEstablished, ConfigValidated:
		return SeverityInfo

	default:
		return SeverityInfo
	}
}

// GetEventCategory returns the category for an event type
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

// IsErrorEvent returns true if the event represents an error condition
func IsErrorEvent(eventType EventType) bool {
	return GetEventSeverity(eventType) == SeverityError
}

// IsWarningEvent returns true if the event represents a warning condition
func IsWarningEvent(eventType EventType) bool {
	return GetEventSeverity(eventType) == SeverityWarning
}

func (w *WebhooksClient) SendBackfillStarted(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, EventBackFillStarted, properties)
}

func (w *WebhooksClient) SendBackfillCompleted(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, EventBackFillCompleted, properties)
}

func (w *WebhooksClient) SendBackfillFailed(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, EventBackFillFailed, properties)
}

func (w *WebhooksClient) SendBackfillProgress(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, BackfillProgress, properties)
}

func (w *WebhooksClient) SendReplicationStarted(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, ReplicationStarted, properties)
}

func (w *WebhooksClient) SendUnableToReplicate(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, UnableToReplicate, properties)
}

func (w *WebhooksClient) SendTableStarted(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, TableStarted, properties)
}

func (w *WebhooksClient) SendTableCompleted(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, TableCompleted, properties)
}

func (w *WebhooksClient) SendTableFailed(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, TableFailed, properties)
}

func (w *WebhooksClient) SendTableSkipped(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, TableSkipped, properties)
}

func (w *WebhooksClient) SendTableEmpty(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, TableEmpty, properties)
}

func (w *WebhooksClient) SendDedupeStarted(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, DedupeStarted, properties)
}

func (w *WebhooksClient) SendDedupeCompleted(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, DedupeCompleted, properties)
}

func (w *WebhooksClient) SendDedupeFailed(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, DedupeFailed, properties)
}

func (w *WebhooksClient) SendConnectionEstablished(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, ConnectionEstablished, properties)
}

func (w *WebhooksClient) SendConnectionLost(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, ConnectionLost, properties)
}

func (w *WebhooksClient) SendConnectionRetry(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, ConnectionRetry, properties)
}

func (w *WebhooksClient) SendConnectionFailed(ctx context.Context, properties map[string]any) error {
	return w.SendEvent(ctx, ConnectionFailed, properties)
}
