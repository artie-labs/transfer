package webhooksutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"time"

	"github.com/artie-labs/transfer/lib/redact"
	"github.com/artie-labs/transfer/lib/stringutil"
)

// WebhooksClient sends events to the webhooks service.
type WebhooksClient struct {
	httpClient http.Client
	apiKey     string
	url        string
	source     Source
	properties map[string]any
}

func NewWebhooksClient(apiKey, url string, source Source, properties map[string]any) (WebhooksClient, error) {
	if stringutil.Empty(apiKey, url) {
		return WebhooksClient{}, fmt.Errorf("apiKey and url are required")
	}

	if properties == nil {
		properties = make(map[string]any)
	}

	return WebhooksClient{
		httpClient: http.Client{
			Timeout: 10 * time.Second,
		},
		apiKey:     apiKey,
		url:        url,
		source:     source,
		properties: properties,
	}, nil
}

func (w WebhooksClient) BuildProperties(eventType EventType, additionalProperties map[string]any) map[string]any {
	props := map[string]any{
		"source":   w.source,
		"message":  GetEventMessage(eventType),
		"severity": GetEventSeverity(eventType),
	}
	maps.Copy(props, w.properties)
	maps.Copy(props, additionalProperties)

	for key, value := range props {
		if strVal, ok := value.(string); ok {
			props[key] = redact.ScrubErrorMessage(strVal)
		}
	}

	return props
}

// SendEvent sends an event to the webhooks service.
func (w WebhooksClient) SendEvent(ctx context.Context, eventType EventType, additionalProperties map[string]any) error {
	event := WebhooksEvent{
		Event:      string(eventType),
		Timestamp:  time.Now().UTC(),
		Properties: w.BuildProperties(eventType, additionalProperties),
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", w.url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", w.apiKey))

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
