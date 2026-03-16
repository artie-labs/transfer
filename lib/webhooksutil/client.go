package webhooksutil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/artie-labs/transfer/lib/redact"
	"github.com/artie-labs/transfer/lib/stringutil"
)

// WebhooksClientConfig holds all configuration for creating a WebhooksClient.
// Using a struct instead of positional parameters prevents accidental argument transposition
// among the many string fields.
type WebhooksClientConfig struct {
	APIKey           string
	URL              string
	Service          Service
	Version          string
	CompanyUUID      string
	PipelineUUID     string
	SourceReaderUUID string
	Source           string // connector source type, e.g. "postgresql"
	Destination      string // connector destination type, e.g. "bigquery"
	Mode             string
}

// WebhooksClient sends events to the webhooks service.
type WebhooksClient struct {
	httpClient http.Client
	cfg        WebhooksClientConfig
}

func NewWebhooksClient(cfg WebhooksClientConfig) (WebhooksClient, error) {
	if stringutil.Empty(cfg.APIKey, cfg.URL) {
		return WebhooksClient{}, fmt.Errorf("apiKey and url are required")
	}

	return WebhooksClient{
		httpClient: http.Client{
			Timeout: 10 * time.Second,
		},
		cfg: cfg,
	}, nil
}

func (w WebhooksClient) BuildProperties(args SendEventArgs) WebhookProperties {
	return WebhookProperties{
		CompanyUUID:      w.cfg.CompanyUUID,
		PipelineUUID:     w.cfg.PipelineUUID,
		SourceReaderUUID: w.cfg.SourceReaderUUID,
		Source:           redact.ScrubString(w.cfg.Source),
		Destination:      redact.ScrubString(w.cfg.Destination),
		Service:          w.cfg.Service,
		Mode:             w.cfg.Mode,
		Version:          w.cfg.Version,
		Error:            redact.ScrubString(args.Error),
		Database:         redact.ScrubString(args.Database),
		Table:            redact.ScrubString(args.Table),
		Schema:           redact.ScrubString(args.Schema),
		Topic:            redact.ScrubString(args.Topic),
		RowsWritten:      args.RowsWritten,
		DurationSeconds:  args.DurationSeconds,
		Reason:           redact.ScrubString(args.Reason),
		PrimaryKeys:      args.PrimaryKeys,
	}
}

// SendEvent sends an event to the webhooks service.
func (w WebhooksClient) SendEvent(ctx context.Context, eventType EventType, args SendEventArgs) error {
	event := WebhooksEvent{
		Event:      string(eventType),
		Timestamp:  time.Now().UTC(),
		MessageID:  uuid.New().String(),
		Properties: w.BuildProperties(args),
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", w.cfg.URL, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", w.cfg.APIKey))

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
