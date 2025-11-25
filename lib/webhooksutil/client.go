package webhooksutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

const (
	envWebhooksAPIKey = "WEBHOOKS_API_KEY"
	envWebhooksURL    = "WEBHOOKS_URL"
)

// WebhooksClient sends events to the webhooks service.
type WebhooksClient struct {
	httpClient  http.Client
	companyUUID string
	dataplane   string
	podID       string
	pipelineID  string
	source      Source
	url         string
	apiKey      string
}

func NewWebhooksClient(companyUUID, dataplane, podID, pipelineID string, source Source) *WebhooksClient {
	apiKey := os.Getenv(envWebhooksAPIKey)
	url := os.Getenv(envWebhooksURL)
	if apiKey == "" || url == "" {
		slog.Warn("Webhooks disabled: missing WEBHOOKS_API_KEY or WEBHOOKS_URL environment variables")
		return nil
	}

	return &WebhooksClient{
		httpClient: http.Client{
			Timeout: 10 * time.Second,
		},
		companyUUID: companyUUID,
		dataplane:   dataplane,
		podID:       podID,
		pipelineID:  pipelineID,
		source:      source,
		url:         url,
		apiKey:      apiKey,
	}
}

// SendEvent sends an event to the webhooks service.
func (c *WebhooksClient) SendEvent(ctx context.Context, eventContext map[string]any, tableID []string, eventType EventType) error {
	if c == nil {
		return fmt.Errorf("webhooks client not initialized")
	}
	if eventContext == nil {
		eventContext = make(map[string]any)
	}

	event := Event{
		PipelineID: c.pipelineID,
		EventType:  eventType,
		Message:    InferMessage(eventType),
		Source:     c.source,
		Timestamp:  time.Now().UTC(),
		Context:    eventContext,
		Severity:   InferSeverity(eventType),
		PodID:      c.podID,
		TableID:    tableID,
	}
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
