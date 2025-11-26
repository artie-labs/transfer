package webhooksutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

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

func (w WebhooksClient) BuildProperties(eventType EventType, tableIDs []string) map[string]any {
	props := w.properties
	props["source"] = w.source
	props["message"] = BuildMessage(eventType)
	props["severity"] = BuildSeverity(eventType)
	props["table_ids"] = tableIDs
	return props
}

// SendEvent sends an event to the webhooks service.
func (w WebhooksClient) SendEvent(ctx context.Context, eventContext map[string]any, tableIDs []string, eventType EventType) error {
	if eventContext == nil {
		eventContext = make(map[string]any)
	}

	event := WebhooksEvent{
		Event:       string(eventType),
		Timestamp:   time.Now().UTC(),
		Properties:  w.BuildProperties(eventType, tableIDs),
		ExtraFields: eventContext,
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
