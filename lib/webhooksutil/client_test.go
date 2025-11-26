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

func (w *WebhooksClientTestSuite) TearDownTest() {
	// No cleanup needed anymore since we don't use environment variables in tests
}

func TestWebhooksClientTestSuite(t *testing.T) {
	suite.Run(t, new(WebhooksClientTestSuite))
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_Success() {
	client, err := NewWebhooksClient("test-api-key", "https://example.com/webhooks", Transfer, map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"})
	assert.NoError(w.T(), err)
	assert.Equal(w.T(), Transfer, client.source)
	assert.Equal(w.T(), "test-api-key", client.apiKey)
	assert.Equal(w.T(), "https://example.com/webhooks", client.url)
	assert.Equal(w.T(), map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"}, client.properties)
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_MissingAPIKey() {
	_, err := NewWebhooksClient("", "https://example.com/webhooks", Transfer, map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"})
	assert.ErrorContains(w.T(), err, "apiKey and url are required")
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_MissingURL() {
	_, err := NewWebhooksClient("test-api-key", "", Transfer, map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"})
	assert.ErrorContains(w.T(), err, "apiKey and url are required")
}

func (w *WebhooksClientTestSuite) TestNewWebhooksClient_MissingBoth() {
	_, err := NewWebhooksClient("", "", Transfer, map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"})
	assert.ErrorContains(w.T(), err, "apiKey and url are required")
}
func (w *WebhooksClientTestSuite) TestSendEvent_Success() {
	// Create a test server
	var receivedEvent WebhooksEvent
	var receivedHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		receivedHeaders = req.Header.Clone()
		err := json.NewDecoder(req.Body).Decode(&receivedEvent)
		assert.NoError(w.T(), err)
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create client with test server URL
	client := WebhooksClient{
		httpClient: http.Client{
			Timeout: 10 * time.Second,
		},
		properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
	}

	eventContext := map[string]any{
		"rows_processed": 100,
		"duration_ms":    5000,
	}
	tableIDs := []string{"schema.table1", "schema.table2"}
	assert.NoError(w.T(), client.SendEvent(w.T().Context(), eventContext, tableIDs, EventBackFillCompleted))

	assert.Equal(w.T(), "pipeline-1", receivedEvent.Properties["pipeline_id"])
	assert.Equal(w.T(), "Backfill completed", receivedEvent.Properties["message"])
	assert.Equal(w.T(), string(Transfer), receivedEvent.Properties["source"])
	assert.Equal(w.T(), string(SeverityInfo), receivedEvent.Properties["severity"])

	// Check table IDs - JSON unmarshalling converts to []any
	tableIDsInterface := receivedEvent.Properties["table_ids"].([]any)
	assert.Len(w.T(), tableIDsInterface, 2)
	assert.Equal(w.T(), "schema.table1", tableIDsInterface[0])
	assert.Equal(w.T(), "schema.table2", tableIDsInterface[1])
	// JSON unmarshalling converts numbers to float64
	assert.Equal(w.T(), float64(100), receivedEvent.ExtraFields["rows_processed"])
	assert.Equal(w.T(), float64(5000), receivedEvent.ExtraFields["duration_ms"])
	assert.WithinDuration(w.T(), time.Now().UTC(), receivedEvent.Timestamp, 2*time.Second)

	// Verify headers
	assert.Equal(w.T(), "application/json", receivedHeaders.Get("Content-Type"))
	assert.Equal(w.T(), "Bearer test-api-key", receivedHeaders.Get("Authorization"))
}

func (w *WebhooksClientTestSuite) TestSendEvent_NilContext() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		var event WebhooksEvent
		err := json.NewDecoder(req.Body).Decode(&event)
		assert.NoError(w.T(), err)
		assert.NotNil(w.T(), event.ExtraFields)
		assert.Empty(w.T(), event.ExtraFields)
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewWebhooksClient("test-api-key", server.URL, Transfer, map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"})
	assert.NoError(w.T(), err)
	client.SendEvent(w.T().Context(), nil, []string{"table1"}, ReplicationStarted)
	assert.NoError(w.T(), err)
}

func (w *WebhooksClientTestSuite) TestSendEvent_HTTPError() {
	// Server that returns 500
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
		source:     Transfer,
		url:        server.URL,
		apiKey:     "test-api-key",
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), nil, []string{"table1"}, EventBackFillFailed), "unexpected status code: 500")
}

func (w *WebhooksClientTestSuite) TestSendEvent_HTTPClientError() {
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
		source:     Transfer,
		url:        server.URL,
		apiKey:     "test-api-key",
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), nil, []string{"table1"}, EventBackFillFailed), "unexpected status code: 400")
}

func (w *WebhooksClientTestSuite) TestSendEvent_ContextCanceled() {
	// Server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		time.Sleep(2 * time.Second)
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
		source:     Transfer,
		url:        server.URL,
		apiKey:     "test-api-key",
	}

	ctx, cancel := context.WithTimeout(w.T().Context(), 100*time.Millisecond)
	defer cancel()

	assert.ErrorContains(w.T(), client.SendEvent(ctx, nil, []string{"table1"}, EventBackFillFailed), "context deadline exceeded")
}

func (w *WebhooksClientTestSuite) TestSendEvent_InvalidURL() {
	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
		source:     Transfer,
		url:        "://invalid-url",
		apiKey:     "test-api-key",
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), nil, []string{"table1"}, EventBackFillFailed), "failed to create request")
}

func (w *WebhooksClientTestSuite) TestSendEvent_NetworkError() {
	// Use a URL that will fail to connect
	client := WebhooksClient{
		httpClient: http.Client{Timeout: 1 * time.Second},
		properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
		source:     Transfer,
		url:        "http://localhost:1", // Port 1 is unlikely to be open
		apiKey:     "test-api-key",
	}

	assert.ErrorContains(w.T(), client.SendEvent(w.T().Context(), nil, []string{"table1"}, EventBackFillFailed), "failed to send request")
}

func (w *WebhooksClientTestSuite) TestSendEvent_AllEventTypes() {
	eventTypes := []struct {
		eventType EventType
		message   string
		severity  Severity
	}{
		{EventBackFillStarted, "Backfill started", SeverityInfo},
		{EventBackFillCompleted, "Backfill completed", SeverityInfo},
		{EventBackFillFailed, "Backfill failed", SeverityError},
		{ReplicationStarted, "Replication started", SeverityInfo},
		{ReplicationFailed, "Replication failed", SeverityError},
		{UnableToReplicate, "Unable to replicate", SeverityError},
	}

	for _, tc := range eventTypes {
		w.T().Run(string(tc.eventType), func(t *testing.T) {
			var receivedEvent WebhooksEvent
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				err := json.NewDecoder(req.Body).Decode(&receivedEvent)
				assert.NoError(t, err)
				rw.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			client := WebhooksClient{
				httpClient: http.Client{Timeout: 10 * time.Second},
				properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
				source:     Debezium,
				url:        server.URL,
				apiKey:     "test-api-key",
			}

			assert.NoError(t, client.SendEvent(w.T().Context(), nil, []string{"table1"}, tc.eventType))
			assert.Equal(t, string(tc.eventType), receivedEvent.Event)
			assert.Equal(t, tc.message, receivedEvent.Properties["message"])
			assert.Equal(t, string(tc.severity), receivedEvent.Properties["severity"])
		})
	}
}

func (w *WebhooksClientTestSuite) TestSendEvent_AllSources() {
	sources := []Source{Transfer, Reader, Debezium, EventsAPI}

	for _, source := range sources {
		w.T().Run(string(source), func(t *testing.T) {
			var receivedEvent WebhooksEvent
			server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
				err := json.NewDecoder(req.Body).Decode(&receivedEvent)
				assert.NoError(t, err)
				rw.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			client := WebhooksClient{
				httpClient: http.Client{Timeout: 10 * time.Second},
				properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
				source:     source,
				url:        server.URL,
				apiKey:     "test-api-key",
			}

			assert.NoError(t, client.SendEvent(w.T().Context(), nil, []string{"table1"}, ReplicationStarted))
			assert.Equal(t, string(source), receivedEvent.Properties["source"])
		})
	}
}

func (w *WebhooksClientTestSuite) TestSendEvent_EmptyTableID() {
	var receivedEvent WebhooksEvent
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		err := json.NewDecoder(req.Body).Decode(&receivedEvent)
		assert.NoError(w.T(), err)
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
		source:     Transfer,
		url:        server.URL,
		apiKey:     "test-api-key",
	}

	assert.NoError(w.T(), client.SendEvent(w.T().Context(), nil, []string{}, EventBackFillStarted))
	assert.Empty(w.T(), receivedEvent.Properties["table_ids"])
}

func (w *WebhooksClientTestSuite) TestSendEvent_NilTableID() {
	var receivedEvent WebhooksEvent
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		err := json.NewDecoder(req.Body).Decode(&receivedEvent)
		assert.NoError(w.T(), err)
		rw.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &WebhooksClient{
		httpClient: http.Client{Timeout: 10 * time.Second},
		properties: map[string]any{"company_uuid": "company-123", "dataplane": "prod", "pipeline_id": "pipeline-1"},
		source:     Transfer,
		url:        server.URL,
		apiKey:     "test-api-key",
	}

	assert.NoError(w.T(), client.SendEvent(w.T().Context(), nil, nil, EventBackFillStarted))
	assert.Nil(w.T(), receivedEvent.Properties["table_ids"])
}
