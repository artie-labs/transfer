package webhooks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/artie-labs/transfer/lib/config"
)

type WebhooksClientTestSuite struct {
	suite.Suite
}

func TestWebhooksClientTestSuite(t *testing.T) {
	suite.Run(t, new(WebhooksClientTestSuite))
}

func newTestClient(t *testing.T, serverURL string, service Service) webhooksClient {
	t.Helper()
	client, err := newWebhooksClient(webhooksClientConfig{
		APIKey:       "test-api-key",
		URL:          serverURL,
		Service:      service,
		Version:      "v1.0.0",
		CompanyUUID:  "company-123",
		PipelineUUID: "pipeline-1",
		Source:       "postgresql",
		Destination:  "bigquery",
		Mode:         "replication",
	})
	assert.NoError(t, err)
	return client
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_Success() {
	client := newTestClient(w.T(), "https://example.com/webhooks", Transfer)
	assert.Equal(w.T(), Transfer, client.cfg.Service)
	assert.Equal(w.T(), "test-api-key", client.cfg.APIKey)
	assert.Equal(w.T(), "https://example.com/webhooks", client.cfg.URL)
	assert.Equal(w.T(), "company-123", client.cfg.CompanyUUID)
	assert.Equal(w.T(), "pipeline-1", client.cfg.PipelineUUID)
	assert.Equal(w.T(), "postgresql", client.cfg.Source)
	assert.Equal(w.T(), "bigquery", client.cfg.Destination)
	assert.Equal(w.T(), "replication", client.cfg.Mode)
	assert.Equal(w.T(), "v1.0.0", client.cfg.Version)
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_MissingAPIKey() {
	_, err := newWebhooksClient(webhooksClientConfig{URL: "https://example.com/webhooks", Service: Transfer})
	assert.ErrorContains(w.T(), err, "apiKey and url are required")
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_MissingURL() {
	_, err := newWebhooksClient(webhooksClientConfig{APIKey: "test-api-key", Service: Transfer})
	assert.ErrorContains(w.T(), err, "apiKey and url are required")
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_MissingBoth() {
	_, err := newWebhooksClient(webhooksClientConfig{Service: Transfer})
	assert.ErrorContains(w.T(), err, "apiKey and url are required")
}

func (w *WebhooksClientTestSuite) TestSendEvent_Success() {
	var receivedEvent WebhooksEvent
	var receivedHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		receivedHeaders = req.Header.Clone()
		assert.NoError(w.T(), json.NewDecoder(req.Body).Decode(&receivedEvent))
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient(w.T(), server.URL, Transfer)

	assert.NoError(w.T(), client.SendEvent(w.T().Context(), EventBackfillCompleted, EventProperties{
		Table:       "my_table",
		Schema:      "public",
		RowsWritten: 100,
	}))

	assert.Equal(w.T(), string(EventBackfillCompleted), receivedEvent.Event)
	assert.WithinDuration(w.T(), time.Now().UTC(), receivedEvent.Timestamp, 2*time.Second)
	assert.NotEmpty(w.T(), receivedEvent.MessageID)

	props := receivedEvent.Properties
	assert.Equal(w.T(), Transfer, props.Service)
	assert.Equal(w.T(), "company-123", props.CompanyUUID)
	assert.Equal(w.T(), "pipeline-1", props.PipelineUUID)
	assert.Equal(w.T(), "postgresql", props.Source)
	assert.Equal(w.T(), "bigquery", props.Destination)
	assert.Equal(w.T(), "replication", props.Mode)
	assert.Equal(w.T(), "v1.0.0", props.Version)
	assert.Equal(w.T(), "my_table", props.Table)
	assert.Equal(w.T(), "public", props.Schema)
	assert.Equal(w.T(), int64(100), props.RowsWritten)

	// Message and Severity are not included in the payload
	assert.Empty(w.T(), props.Error)

	// Verify headers
	assert.Equal(w.T(), "application/json", receivedHeaders.Get("Content-Type"))
	assert.Equal(w.T(), "Bearer test-api-key", receivedHeaders.Get("Authorization"))
}

func (w *WebhooksClientTestSuite) TestSendEvent_NilContext() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		var event WebhooksEvent
		assert.NoError(w.T(), json.NewDecoder(req.Body).Decode(&event))
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient(w.T(), server.URL, Transfer)
	assert.NoError(w.T(), client.SendEvent(w.T().Context(), EventReplicationStarted, EventProperties{}))
}

func (w *WebhooksClientTestSuite) TestSendEvent_HTTPError() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := webhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		cfg:        webhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: server.URL, APIKey: "test-api-key"},
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), EventBackfillFailed, EventProperties{}), "unexpected status code: 500")
}

func (w *WebhooksClientTestSuite) TestSendEvent_HTTPClientError() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client := webhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		cfg:        webhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: server.URL, APIKey: "test-api-key"},
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), EventBackfillFailed, EventProperties{}), "unexpected status code: 400")
}

func (w *WebhooksClientTestSuite) TestSendEvent_ContextCanceled() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		time.Sleep(2 * time.Second)
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := webhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		cfg:        webhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: server.URL, APIKey: "test-api-key"},
	}

	ctx, cancel := context.WithTimeout(w.T().Context(), 100*time.Millisecond)
	defer cancel()

	assert.ErrorContains(w.T(), client.SendEvent(ctx, EventBackfillFailed, EventProperties{}), "context deadline exceeded")
}

func (w *WebhooksClientTestSuite) TestSendEvent_InvalidURL() {
	client := webhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		cfg:        webhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: "://invalid-url", APIKey: "test-api-key"},
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), EventBackfillFailed, EventProperties{}), "failed to create request")
}

func (w *WebhooksClientTestSuite) TestSendEvent_NetworkError() {
	client := webhooksClient{
		httpClient: http.Client{Timeout: 1 * time.Second},
		cfg:        webhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: "http://localhost:1", APIKey: "test-api-key"},
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), EventBackfillFailed, EventProperties{}), "failed to send request")
}

func (w *WebhooksClientTestSuite) TestSendEvent_AllEventTypes() {
	for _, eventType := range AllEventTypes {
		w.T().Run(string(eventType), func(t *testing.T) {
			var receivedEvent WebhooksEvent
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				err := json.NewDecoder(req.Body).Decode(&receivedEvent)
				assert.NoError(t, err)
				rw.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL, Debezium)
			assert.NoError(t, client.SendEvent(w.T().Context(), eventType, EventProperties{}))
			assert.Equal(t, string(eventType), receivedEvent.Event)
			// Message and Severity are derived by dashboard — not in payload
			assert.Empty(t, receivedEvent.Properties.Error)
		})
	}
}

func (w *WebhooksClientTestSuite) TestSendEvent_AllServices() {
	for _, service := range []Service{Transfer, Reader, Debezium} {
		w.T().Run(string(service), func(t *testing.T) {
			var receivedEvent WebhooksEvent
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				assert.NoError(t, json.NewDecoder(req.Body).Decode(&receivedEvent))
				rw.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL, service)
			assert.NoError(t, client.SendEvent(w.T().Context(), EventReplicationStarted, EventProperties{}))
			assert.Equal(t, service, receivedEvent.Properties.Service)
		})
	}
}

func (w *WebhooksClientTestSuite) TestBuildProperties_ErrorConsolidation() {
	client := webhooksClient{
		cfg: webhooksClientConfig{
			Service:     Transfer,
			CompanyUUID: "company-123",
			Source:      "postgresql",
			Destination: "bigquery",
			Mode:        "replication",
			Version:     "v1.2.3",
		},
	}

	props := client.buildProperties(EventProperties{
		Error: "Failed to replicate: connection timeout",
		Table: "users",
	})

	assert.Equal(w.T(), "Failed to replicate: connection timeout", props.Error)
	assert.Empty(w.T(), props.Details) // Details not set by new callers
	assert.Equal(w.T(), "users", props.Table)
	assert.Equal(w.T(), Transfer, props.Service)
}

func (w *WebhooksClientTestSuite) TestSendEvent_EmptyArgs() {
	var receivedEvent WebhooksEvent
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		err := json.NewDecoder(req.Body).Decode(&receivedEvent)
		assert.NoError(w.T(), err)
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestClient(w.T(), server.URL, Transfer)
	assert.NoError(w.T(), client.SendEvent(w.T().Context(), EventBackfillStarted, EventProperties{}))
	assert.Empty(w.T(), receivedEvent.Properties.Table)
	assert.Empty(w.T(), receivedEvent.Properties.Error)
}

// Tests for the high-level Client wrapper.

func TestNewFromConfig(t *testing.T) {
	{
		// nil config returns no-op client
		client, err := NewClient(nil, Transfer, "v1.0.0")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.False(t, client.IsEnabled())
	}
	{
		// disabled config returns no-op client
		client, err := NewClient(&config.WebhookSettings{
			Enabled: false,
			URL:     "https://example.com",
			APIKey:  "test-key",
		}, Transfer, "v1.0.0")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.False(t, client.IsEnabled())
	}
	{
		// enabled config missing API key returns error
		client, err := NewClient(&config.WebhookSettings{
			Enabled: true,
			URL:     "https://example.com",
			APIKey:  "",
		}, Transfer, "v1.0.0")
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// enabled config missing URL returns error
		client, err := NewClient(&config.WebhookSettings{
			Enabled: true,
			URL:     "",
			APIKey:  "test-key",
		}, Transfer, "v1.0.0")
		assert.Error(t, err)
		assert.Nil(t, client)
	}
	{
		// valid enabled config
		client, err := NewClient(&config.WebhookSettings{
			Enabled:     true,
			URL:         "https://example.com/webhook",
			APIKey:      "test-api-key",
			CompanyUUID: "company-123",
			Source:      "postgresql",
			Destination: "bigquery",
			Mode:        "replication",
		}, Transfer, "v1.0.0")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.IsEnabled())
		assert.NotNil(t, client.inner)
	}
	{
		// service parameter is passed through correctly
		client, err := NewClient(&config.WebhookSettings{
			Enabled:     true,
			URL:         "https://example.com/webhook",
			APIKey:      "test-api-key",
			CompanyUUID: "company-123",
		}, Reader, "v2.0.0")
		assert.NoError(t, err)
		assert.NotNil(t, client)
		assert.True(t, client.IsEnabled())
		assert.Equal(t, Reader, client.inner.cfg.Service)
		assert.Equal(t, "v2.0.0", client.inner.cfg.Version)
	}
}

func TestClient_IsEnabled(t *testing.T) {
	{
		// nil client
		var client *Client
		assert.False(t, client.IsEnabled())
	}
	{
		// no-op client (inner is nil)
		client := &Client{}
		assert.False(t, client.IsEnabled())
	}
	{
		// enabled client
		inner := webhooksClient{}
		client := &Client{inner: &inner}
		assert.True(t, client.IsEnabled())
	}
}

func TestClient_SendEvent(t *testing.T) {
	ctx := context.Background()
	{
		// nil client should not panic
		var client *Client
		assert.NotPanics(t, func() {
			client.SendEvent(ctx, EventBackfillStarted, EventProperties{Table: "users"})
		})
	}
	{
		// disabled (no-op) client should not panic
		client := &Client{}
		assert.NotPanics(t, func() {
			client.SendEvent(ctx, EventBackfillStarted, EventProperties{Table: "users"})
		})
	}
	{
		// enabled client with all event types should not panic
		inner := webhooksClient{}
		client := &Client{inner: &inner}
		for _, eventType := range AllEventTypes {
			assert.NotPanics(t, func() {
				client.SendEvent(ctx, eventType, EventProperties{})
			})
		}
	}
}
