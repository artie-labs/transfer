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

func new(apiKey, url string, properties map[string]any) (*Client, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("webhook apiKey is required")
	}

	if url == "" {
		return nil, fmt.Errorf("webhook url is required")
	}

	client, err := webhooksutil.NewWebhooksClient(apiKey, url, webhooksutil.Transfer, properties)
	if err != nil {
		return nil, fmt.Errorf("failed to create webhooks client: %w", err)
	}

	return &Client{
		client:  &client,
		enabled: true,
	}, nil
}

func NewFromConfig(cfg *config.WebhookSettings) (*Client, error) {
	if cfg == nil || !cfg.Enabled {
		return &Client{}, nil
	}

	return new(cfg.APIKey, cfg.URL, cfg.Properties)
}

func (c *Client) IsEnabled() bool {
	return c != nil && c.enabled && c.client != nil
}

func (c *Client) SendEvent(ctx context.Context, eventType webhooksutil.EventType, properties map[string]any) {
	if !c.IsEnabled() {
		return
	}
	if err := c.client.SendEvent(ctx, eventType, properties); err != nil {
		slog.Warn("Failed to send webhook event", slog.String("event", string(eventType)), slog.Any("err", err))
	}
}
