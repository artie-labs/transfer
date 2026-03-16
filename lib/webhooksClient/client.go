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

func new(apiKey, url string, cfg *config.WebhookSettings, version string) (*Client, error) {
	client, err := webhooksutil.NewWebhooksClient(
		apiKey,
		url,
		webhooksutil.Transfer,
		version,
		cfg.CompanyUUID,
		cfg.PipelineUUID,
		cfg.SourceReaderUUID,
		cfg.Source,
		cfg.Destination,
		cfg.Mode,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create webhooks client: %w", err)
	}

	return &Client{
		client:  &client,
		enabled: true,
	}, nil
}

func NewFromConfig(cfg *config.WebhookSettings, version string) (*Client, error) {
	if cfg == nil || !cfg.Enabled {
		return &Client{}, nil
	}

	return new(cfg.APIKey, cfg.URL, cfg, version)
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
