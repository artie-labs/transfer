package webhooks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInferSeverity(t *testing.T) {
	expectedMap := map[EventType]Severity{
		EventBackfillStarted:    SeverityInfo,
		EventBackfillCompleted:  SeverityInfo,
		EventBackfillFailed:     SeverityError,
		EventReplicationStarted: SeverityInfo,
		EventReplicationFailed:  SeverityError,
		EventType("unknown"):    SeverityInfo,
	}

	for eventType, expectedSeverity := range expectedMap {
		assert.Equal(t, expectedSeverity, GetEventSeverity(eventType))
	}
}

func TestInferMessage(t *testing.T) {
	expectedMap := map[EventType]string{
		EventBackfillStarted:    "Backfill started",
		EventBackfillCompleted:  "Backfill completed",
		EventBackfillFailed:     "Backfill failed",
		EventReplicationStarted: "Replication started",
		EventReplicationFailed:  "Replication failed",
		EventType("unknown"):    "Unknown event type",
	}

	for eventType, expectedMessage := range expectedMap {
		assert.Equal(t, expectedMessage, GetEventMessage(eventType))
	}
}
