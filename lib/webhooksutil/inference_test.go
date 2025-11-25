package webhooksutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInferSeverity(t *testing.T) {
	tests := []struct {
		name      string
		eventType EventType
		expected  Severity
	}{
		{
			name:      "backfill started",
			eventType: EventBackFillStarted,
			expected:  SeverityInfo,
		},
		{
			name:      "backfill completed",
			eventType: EventBackFillCompleted,
			expected:  SeverityInfo,
		},
		{
			name:      "backfill failed",
			eventType: EventBackFillFailed,
			expected:  SeverityError,
		},
		{
			name:      "replication started",
			eventType: ReplicationStarted,
			expected:  SeverityInfo,
		},
		{
			name:      "replication failed",
			eventType: ReplicationFailed,
			expected:  SeverityError,
		},
		{
			name:      "unable to replicate",
			eventType: UnableToReplicate,
			expected:  SeverityError,
		},
		{
			name:      "unknown event type",
			eventType: EventType("unknown"),
			expected:  SeverityInfo,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferSeverity(tt.eventType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestInferMessage(t *testing.T) {
	tests := []struct {
		name      string
		eventType EventType
		expected  string
	}{
		{
			name:      "backfill started",
			eventType: EventBackFillStarted,
			expected:  "Backfill started",
		},
		{
			name:      "backfill completed",
			eventType: EventBackFillCompleted,
			expected:  "Backfill completed",
		},
		{
			name:      "backfill failed",
			eventType: EventBackFillFailed,
			expected:  "Backfill failed",
		},
		{
			name:      "replication started",
			eventType: ReplicationStarted,
			expected:  "Replication started",
		},
		{
			name:      "replication failed",
			eventType: ReplicationFailed,
			expected:  "Replication failed",
		},
		{
			name:      "unable to replicate",
			eventType: UnableToReplicate,
			expected:  "Unable to replicate",
		},
		{
			name:      "unknown event type",
			eventType: EventType("unknown"),
			expected:  "Unknown event type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InferMessage(tt.eventType)
			assert.Equal(t, tt.expected, result)
		})
	}
}
