package eventtracking

import (
	"encoding/json"
	"fmt"

	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type Format struct{}

func (Format) GetEventFromBytes(bytes []byte) (cdc.Event, error) {
	if len(bytes) == 0 {
		return nil, fmt.Errorf("empty message")
	}

	var payload EventPayload
	if err := json.Unmarshal(bytes, &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %w", err)
	}

	// Validate required fields
	if payload.Event == "" {
		return nil, fmt.Errorf("missing required field: event")
	}
	if payload.Properties == nil {
		return nil, fmt.Errorf("missing required field: properties")
	}
	if payload.Timestamp == "" {
		return nil, fmt.Errorf("missing required field: timestamp")
	}
	if payload.MessageID == "" {
		return nil, fmt.Errorf("missing required field: messageID")
	}

	// Parse the JSON again, this time as a map to capture additional top-level fields
	var rawPayload map[string]any
	if err := json.Unmarshal(bytes, &rawPayload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal raw json: %w", err)
	}

	// Extract additional fields (excluding known fields)
	knownFields := map[string]bool{
		"event":      true,
		"properties": true,
		"timestamp":  true,
		"messageID":  true,
	}
	payload.additionalFields = make(map[string]any)
	for k, v := range rawPayload {
		if !knownFields[k] {
			payload.additionalFields[k] = v
		}
	}

	return &EventTrackingEvent{payload: payload}, nil
}

func (Format) Labels() []string {
	return []string{constants.EventTrackingFormat}
}

func (Format) GetPrimaryKey(key []byte, tc kafkalib.TopicConfig, reservedColumns map[string]bool) (map[string]any, error) {
	return map[string]any{
		columns.EscapeName("id", reservedColumns): string(key),
	}, nil
}
