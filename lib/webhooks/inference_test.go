package webhooks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInferSeverity(t *testing.T) {
	expectedMap := map[EventType]Severity{
		EventBackFillStarted:   SeverityInfo,
		EventBackFillCompleted: SeverityInfo,
		EventBackFillFailed:    SeverityError,
		ReplicationStarted:     SeverityInfo,
		ReplicationFailed:      SeverityError,
		EventType("unknown"):   SeverityInfo,
	}

	for eventType, expectedSeverity := range expectedMap {
		assert.Equal(t, expectedSeverity, GetEventSeverity(eventType))
	}
}

func TestInferMessage(t *testing.T) {
	expectedMap := map[EventType]string{
		EventBackFillStarted:   "Backfill started",
		EventBackFillCompleted: "Backfill completed",
		EventBackFillFailed:    "Backfill failed",
		ReplicationStarted:     "Replication started",
		ReplicationFailed:      "Replication failed",
		EventType("unknown"):   "Unknown event type",
	}

	for eventType, expectedMessage := range expectedMap {
		assert.Equal(t, expectedMessage, GetEventMessage(eventType))
	}
}
