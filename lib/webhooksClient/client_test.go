package webhooksclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/webhooksutil"
)

func TestNewFromConfig(t *testing.T) {
	{
		// nil config
		client, err := NewFromConfig(nil)
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.False(t, client.enabled)
		assert.Nil(t, client.client)
	}
	{
		// disabled config
		client, err := NewFromConfig(&config.WebhookSettings{
			Enabled: false,
			URL:     "https://example.com",
			APIKey:  "test-key",
		})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.False(t, client.enabled)
		assert.Nil(t, client.client)
	}
	{
		// enabled config missing API key
		client, err := NewFromConfig(&config.WebhookSettings{
			Enabled: true,
			URL:     "https://example.com",
			APIKey:  "",
		})
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// enabled config missing URL
		client, err := NewFromConfig(&config.WebhookSettings{
			Enabled: true,
			URL:     "",
			APIKey:  "test-key",
		})
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// valid enabled config
		client, err := NewFromConfig(&config.WebhookSettings{
			Enabled: true,
			URL:     "https://example.com/webhook",
			APIKey:  "test-api-key",
			Properties: map[string]any{
				"environment": "test",
				"version":     "1.0.0",
			},
		})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.enabled)
		assert.NotNil(t, client.client)
	}
	{
		// valid enabled config without properties
		client, err := NewFromConfig(&config.WebhookSettings{
			Enabled: true,
			URL:     "https://example.com/webhook",
			APIKey:  "test-api-key",
		})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.enabled)
		assert.NotNil(t, client.client)
	}
}

func TestClient_IsEnabled(t *testing.T) {
	{
		// nil client
		var client *Client
		assert.False(t, client.IsEnabled())
	}
	{
		// disabled client
		client := &Client{
			enabled: false,
			client:  nil,
		}
		assert.False(t, client.IsEnabled())
	}
	{
		// enabled but nil webhook client
		client := &Client{
			enabled: true,
			client:  nil,
		}
		assert.False(t, client.IsEnabled())
	}
	{
		// enabled with valid webhook client
		client := &Client{
			enabled: true,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.True(t, client.IsEnabled())
	}
	{
		// disabled with valid webhook client
		client := &Client{
			enabled: false,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.False(t, client.IsEnabled())
	}
}

func TestClient_SendEvent(t *testing.T) {
	{
		// nil client
		var client *Client
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.TableStarted, map[string]any{"table": "users"})
		})
	}
	{
		// disabled client
		client := &Client{
			enabled: false,
			client:  nil,
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.TableStarted, map[string]any{"table": "users"})
		})
	}
	{
		// enabled but nil webhook client
		client := &Client{
			enabled: true,
			client:  nil,
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.TableStarted, map[string]any{"table": "users"})
		})
	}
	{
		// enabled with valid webhook client
		client := &Client{
			enabled: true,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.TableStarted, map[string]any{"table": "users"})
		})
	}
	{
		// send event with nil properties
		client := &Client{
			enabled: true,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.EventBackFillStarted, nil)
		})
	}
	{
		// send event with empty properties
		client := &Client{
			enabled: true,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.EventBackFillCompleted, map[string]any{})
		})
	}
	{
		// send event with complex properties
		client := &Client{
			enabled: true,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.BackfillProgress, map[string]any{
				"rowsWritten":         1000,
				"duration":            "5m",
				"throughputPerSecond": 3.33,
			})
		})
	}
}

func TestClient_SendEvent_AllEventTypes(t *testing.T) {
	// Test that all event types can be sent without panicking
	client := &Client{
		enabled: true,
		client:  &webhooksutil.WebhooksClient{},
	}

	ctx := context.Background()
	properties := map[string]any{
		"test": "value",
	}

	for _, eventType := range webhooksutil.AllEventTypes {
		assert.NotPanics(t, func() {
			client.SendEvent(ctx, eventType, properties)
		})
	}
}

func TestNew(t *testing.T) {
	{
		// empty API key
		client, err := new("", "https://example.com", nil)
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// empty URL
		client, err := new("test-key", "", nil)
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// both empty
		client, err := new("", "", nil)
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// valid inputs
		client, err := new("test-api-key", "https://example.com/webhook", map[string]any{"env": "test"})
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.enabled)
		assert.NotNil(t, client.client)
	}
	{
		// valid inputs with nil properties
		client, err := new("test-api-key", "https://example.com/webhook", nil)
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.enabled)
		assert.NotNil(t, client.client)
	}
}
