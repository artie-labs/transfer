package webhooks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetEventSeverity(t *testing.T) {
	expectedMap := map[EventType]Severity{
		EventBackfillStarted:    SeverityInfo,
		EventBackfillCompleted:  SeverityInfo,
		EventBackfillFailed:     SeverityError,
		EventReplicationStarted: SeverityInfo,
		EventReplicationError:   SeverityError,
		EventType("unknown"):    SeverityInfo,
	}

	for eventType, expectedSeverity := range expectedMap {
		assert.Equal(t, expectedSeverity, GetEventSeverity(eventType))
	}
}

func TestGetEventMessage(t *testing.T) {
	expectedMap := map[EventType]string{
		EventBackfillStarted:    "Backfill started",
		EventBackfillCompleted:  "Backfill completed",
		EventBackfillFailed:     "Backfill failed",
		EventReplicationStarted: "Replication started",
		EventReplicationError:   "Replication error",
		EventType("unknown"):    "Unknown event type",
	}

	for eventType, expectedMessage := range expectedMap {
		assert.Equal(t, expectedMessage, GetEventMessage(eventType))
	}
}

func TestAllEventTypes(t *testing.T) {
	// Check for duplicates in AllEventTypes.
	seen := make(map[EventType]bool)
	for _, et := range AllEventTypes {
		assert.False(t, seen[et], "duplicate event type in AllEventTypes: %s", et)
		seen[et] = true
	}

	// Every entry in AllEventTypes must have a corresponding entry in eventMetadataMap.
	for _, et := range AllEventTypes {
		_, ok := eventMetadataMap[et]
		assert.True(t, ok, "event type %q is in AllEventTypes but missing from eventMetadataMap", et)
	}

	// Every entry in eventMetadataMap must be present in AllEventTypes.
	allSet := make(map[EventType]bool)
	for _, et := range AllEventTypes {
		allSet[et] = true
	}
	for et := range eventMetadataMap {
		assert.True(t, allSet[et], "event type %q is in eventMetadataMap but missing from AllEventTypes", et)
	}
}
