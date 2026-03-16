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

// WebhooksClient sends events to the webhooks service.
type WebhooksClient struct {
	httpClient       http.Client
	apiKey           string
	url              string
	service          Service
	version          string
	companyUUID      string
	pipelineUUID     string
	sourceReaderUUID string
	source           string // connector source type, e.g. "postgresql"
	destination      string // connector destination type, e.g. "bigquery"
	mode             string
}

func NewWebhooksClient(apiKey, url string, service Service, version, companyUUID, pipelineUUID, sourceReaderUUID, source, destination, mode string) (WebhooksClient, error) {
	if stringutil.Empty(apiKey, url) {
		return WebhooksClient{}, fmt.Errorf("apiKey and url are required")
	}

	return WebhooksClient{
		httpClient: http.Client{
			Timeout: 10 * time.Second,
		},
		apiKey:           apiKey,
		url:              url,
		service:          service,
		version:          version,
		companyUUID:      companyUUID,
		pipelineUUID:     pipelineUUID,
		sourceReaderUUID: sourceReaderUUID,
		source:           source,
		destination:      destination,
		mode:             mode,
	}, nil
}

func (w WebhooksClient) BuildProperties(args SendEventArgs) WebhookProperties {
	return WebhookProperties{
		CompanyUUID:      w.companyUUID,
		PipelineUUID:     w.pipelineUUID,
		SourceReaderUUID: w.sourceReaderUUID,
		Source:           redact.ScrubString(w.source),
		Destination:      redact.ScrubString(w.destination),
		Service:          w.service,
		Mode:             w.mode,
		Version:          w.version,
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
