package webhooksutil

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type WebhooksClientTestSuite struct {
	suite.Suite
}

func TestWebhooksClientTestSuite(t *testing.T) {
	suite.Run(t, new(WebhooksClientTestSuite))
}

func newTestClient(t *testing.T, serverURL string, service Service) WebhooksClient {
	t.Helper()
	client, err := NewWebhooksClient(WebhooksClientConfig{
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
	client, err := NewWebhooksClient(WebhooksClientConfig{
		APIKey:       "test-api-key",
		URL:          "https://example.com/webhooks",
		Service:      Transfer,
		Version:      "v1.0.0",
		CompanyUUID:  "company-123",
		PipelineUUID: "pipeline-1",
		Source:       "postgresql",
		Destination:  "bigquery",
		Mode:         "replication",
	})
	assert.NoError(w.T(), err)
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
	_, err := NewWebhooksClient(WebhooksClientConfig{URL: "https://example.com/webhooks", Service: Transfer})
	assert.ErrorContains(w.T(), err, "apiKey and url are required")
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_MissingURL() {
	_, err := NewWebhooksClient(WebhooksClientConfig{APIKey: "test-api-key", Service: Transfer})
	assert.ErrorContains(w.T(), err, "apiKey and url are required")
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_MissingBoth() {
	_, err := NewWebhooksClient(WebhooksClientConfig{Service: Transfer})
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

	assert.NoError(w.T(), client.SendEvent(w.T().Context(), EventBackFillCompleted, SendEventArgs{
		Table:       "my_table",
		Schema:      "public",
		RowsWritten: 100,
	}))

	assert.Equal(w.T(), string(EventBackFillCompleted), receivedEvent.Event)
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
	assert.NoError(w.T(), client.SendEvent(w.T().Context(), ReplicationStarted, SendEventArgs{}))
}

func (w *WebhooksClientTestSuite) TestSendEvent_HTTPError() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		cfg:        WebhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: server.URL, APIKey: "test-api-key"},
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), EventBackFillFailed, SendEventArgs{}), "unexpected status code: 500")
}

func (w *WebhooksClientTestSuite) TestSendEvent_HTTPClientError() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		cfg:        WebhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: server.URL, APIKey: "test-api-key"},
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), EventBackFillFailed, SendEventArgs{}), "unexpected status code: 400")
}

func (w *WebhooksClientTestSuite) TestSendEvent_ContextCanceled() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		time.Sleep(2 * time.Second)
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		cfg:        WebhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: server.URL, APIKey: "test-api-key"},
	}

	ctx, cancel := context.WithTimeout(w.T().Context(), 100*time.Millisecond)
	defer cancel()

	assert.ErrorContains(w.T(), client.SendEvent(ctx, EventBackFillFailed, SendEventArgs{}), "context deadline exceeded")
}

func (w *WebhooksClientTestSuite) TestSendEvent_InvalidURL() {
	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		cfg:        WebhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: "://invalid-url", APIKey: "test-api-key"},
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), EventBackFillFailed, SendEventArgs{}), "failed to create request")
}

func (w *WebhooksClientTestSuite) TestSendEvent_NetworkError() {
	client := WebhooksClient{
		httpClient: http.Client{Timeout: 1 * time.Second},
		cfg:        WebhooksClientConfig{Service: Transfer, CompanyUUID: "company-123", URL: "http://localhost:1", APIKey: "test-api-key"},
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), EventBackFillFailed, SendEventArgs{}), "failed to send request")
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
			assert.NoError(t, client.SendEvent(w.T().Context(), eventType, SendEventArgs{}))
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
			assert.NoError(t, client.SendEvent(w.T().Context(), ReplicationStarted, SendEventArgs{}))
			assert.Equal(t, service, receivedEvent.Properties.Service)
		})
	}
}

func (w *WebhooksClientTestSuite) TestBuildProperties_ErrorConsolidation() {
	client := WebhooksClient{
		cfg: WebhooksClientConfig{
			Service:     Transfer,
			CompanyUUID: "company-123",
			Source:      "postgresql",
			Destination: "bigquery",
			Mode:        "replication",
			Version:     "v1.2.3",
		},
	}

	props := client.BuildProperties(SendEventArgs{
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
	assert.NoError(w.T(), client.SendEvent(w.T().Context(), EventBackFillStarted, SendEventArgs{}))
	assert.Empty(w.T(), receivedEvent.Properties.Table)
	assert.Empty(w.T(), receivedEvent.Properties.Error)
}
