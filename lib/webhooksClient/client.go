package webhooksclient

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/webhooksutil"
)

type Client struct {
	client  *webhooksutil.WebhooksClient
	enabled bool
}

func NewFromConfig(cfg *config.WebhookSettings, version string) (*Client, error) {
	if cfg == nil || !cfg.Enabled {
		return &Client{}, nil
	}

	client, err := webhooksutil.NewWebhooksClient(webhooksutil.WebhooksClientConfig{
		APIKey:           cfg.APIKey,
		URL:              cfg.URL,
		Service:          webhooksutil.Transfer,
		Version:          version,
		CompanyUUID:      cfg.CompanyUUID,
		PipelineUUID:     cfg.PipelineUUID,
		SourceReaderUUID: cfg.SourceReaderUUID,
		Source:           cfg.Source,
		Destination:      cfg.Destination,
		Mode:             cfg.Mode,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create webhooks client: %w", err)
	}

	return &Client{
		client:  &client,
		enabled: true,
	}, nil
}

func (c *Client) IsEnabled() bool {
	return c != nil && c.enabled && c.client != nil
}

func (c *Client) SendEvent(ctx context.Context, eventType webhooksutil.EventType, args webhooksutil.SendEventArgs) {
	if !c.IsEnabled() {
		return
	}
	if err := c.client.SendEvent(ctx, eventType, args); err != nil {
		slog.Error("Failed to send webhook event", slog.String("event", string(eventType)), slog.Any("err", err))
	}
}
