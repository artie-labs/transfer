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
		client, err := NewFromConfig(nil, "v1.0.0")
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
		}, "v1.0.0")
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
		}, "v1.0.0")
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// enabled config missing URL
		client, err := NewFromConfig(&config.WebhookSettings{
			Enabled: true,
			URL:     "",
			APIKey:  "test-key",
		}, "v1.0.0")
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// valid enabled config with typed fields
		client, err := NewFromConfig(&config.WebhookSettings{
			Enabled:     true,
			URL:         "https://example.com/webhook",
			APIKey:      "test-api-key",
			CompanyUUID: "company-123",
			Source:      "postgresql",
			Destination: "bigquery",
			Mode:        "replication",
		}, "v1.0.0")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.enabled)
		assert.NotNil(t, client.client)
	}
	{
		// valid enabled config without optional fields
		client, err := NewFromConfig(&config.WebhookSettings{
			Enabled:     true,
			URL:         "https://example.com/webhook",
			APIKey:      "test-api-key",
			CompanyUUID: "company-123",
		}, "v1.0.0")
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
			client.SendEvent(context.Background(), webhooksutil.TableStarted, webhooksutil.SendEventArgs{Table: "users"})
		})
	}
	{
		// disabled client
		client := &Client{
			enabled: false,
			client:  nil,
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.TableStarted, webhooksutil.SendEventArgs{Table: "users"})
		})
	}
	{
		// enabled but nil webhook client
		client := &Client{
			enabled: true,
			client:  nil,
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.TableStarted, webhooksutil.SendEventArgs{Table: "users"})
		})
	}
	{
		// enabled with valid webhook client
		client := &Client{
			enabled: true,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.TableStarted, webhooksutil.SendEventArgs{Table: "users"})
		})
	}
	{
		// send event with empty args
		client := &Client{
			enabled: true,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.EventBackFillStarted, webhooksutil.SendEventArgs{})
		})
	}
	{
		// send event with complex args
		client := &Client{
			enabled: true,
			client:  &webhooksutil.WebhooksClient{},
		}
		assert.NotPanics(t, func() {
			client.SendEvent(context.Background(), webhooksutil.BackfillProgress, webhooksutil.SendEventArgs{
				Table:           "users",
				Schema:          "public",
				RowsWritten:     1000,
				DurationSeconds: 300.0,
			})
		})
	}
}

func TestClient_SendEvent_AllEventTypes(t *testing.T) {
	client := &Client{
		enabled: true,
		client:  &webhooksutil.WebhooksClient{},
	}

	ctx := context.Background()
	for _, eventType := range webhooksutil.AllEventTypes {
		assert.NotPanics(t, func() {
			client.SendEvent(ctx, eventType, webhooksutil.SendEventArgs{})
		})
	}
}

func TestNew(t *testing.T) {
	{
		// empty API key
		client, err := new("", "https://example.com", &config.WebhookSettings{}, "v1.0.0")
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// empty URL
		client, err := new("test-key", "", &config.WebhookSettings{}, "v1.0.0")
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// both empty
		client, err := new("", "", &config.WebhookSettings{}, "v1.0.0")
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// valid inputs
		client, err := new("test-api-key", "https://example.com/webhook", &config.WebhookSettings{
			CompanyUUID: "company-123",
			Source:      "postgresql",
			Destination: "bigquery",
		}, "v1.0.0")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.enabled)
		assert.NotNil(t, client.client)
	}
	{
		// valid inputs with empty optional fields
		client, err := new("test-api-key", "https://example.com/webhook", &config.WebhookSettings{
			CompanyUUID: "company-123",
		}, "v1.0.0")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.enabled)
		assert.NotNil(t, client.client)
	}
}
