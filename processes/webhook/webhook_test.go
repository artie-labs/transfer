package webhook

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	cfg := Config{
		URLs: []string{"http://example.com/webhook"},
	}

	svc := NewService(cfg)
	assert.NotNil(t, svc)
	assert.NotNil(t, svc.client)
	assert.Equal(t, 1000, svc.config.BufferSize)
}

func TestPublish(t *testing.T) {
	cfg := Config{
		URLs:       []string{"http://example.com/webhook"},
		BufferSize: 10,
	}

	svc := NewService(cfg)

	// Test successful publish
	svc.Publish(EventMergeStarted, StatusSuccess, map[string]any{"table": "users"})

	// Verify action was queued
	select {
	case action := <-svc.actions:
		assert.Equal(t, EventMergeStarted, action.Event)
		assert.Equal(t, StatusSuccess, action.Status)
		assert.Equal(t, "users", action.Metadata["table"])
	case <-time.After(time.Second):
		t.Fatal("Action was not queued")
	}
}

func TestPublish_FullBuffer(t *testing.T) {
	cfg := Config{
		URLs:       []string{"http://example.com/webhook"},
		BufferSize: 1,
	}

	svc := NewService(cfg)

	// Fill the buffer
	svc.Publish(EventMergeStarted, StatusSuccess, nil)

	// This should not block and should drop the event
	done := make(chan bool)
	go func() {
		svc.Publish(EventMergeFinished, StatusSuccess, nil)
		done <- true
	}()

	select {
	case <-done:
		// Successfully returned without blocking
	case <-time.After(time.Second):
		t.Fatal("Publish blocked when buffer was full")
	}
}

func TestDeliver_Success(t *testing.T) {
	var receivedPayload Action
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "Artie-Transfer-Webhook/1.0", r.Header.Get("User-Agent"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		mu.Lock()
		err = json.Unmarshal(body, &receivedPayload)
		mu.Unlock()
		require.NoError(t, err)

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := Config{
		URLs: []string{server.URL},
	}
	svc := NewService(cfg)

	action := Action{
		Event:    EventMergeStarted,
		Status:   StatusSuccess,
		Metadata: map[string]any{"table": "orders", "rows": 100},
	}

	err := svc.deliver(context.Background(), server.URL, action, 0)
	require.NoError(t, err)

	mu.Lock()
	assert.Equal(t, EventMergeStarted, receivedPayload.Event)
	assert.Equal(t, StatusSuccess, receivedPayload.Status)
	assert.Equal(t, "orders", receivedPayload.Metadata["table"])
	assert.Equal(t, float64(100), receivedPayload.Metadata["rows"]) // JSON unmarshals numbers as float64
	mu.Unlock()
}

func TestDeliver_Retry5xx(t *testing.T) {
	attemptCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attemptCount++
		currentAttempt := attemptCount
		mu.Unlock()

		if currentAttempt < 3 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	cfg := Config{
		URLs: []string{server.URL},
	}
	svc := NewService(cfg)

	action := Action{
		Event:  EventMergeFinished,
		Status: StatusSuccess,
	}

	err := svc.deliverWithRetry(context.Background(), server.URL, action)
	require.NoError(t, err)

	mu.Lock()
	assert.Equal(t, 3, attemptCount, "Should have retried until success")
	mu.Unlock()
}

func TestDeliver_NoRetry4xx(t *testing.T) {
	attemptCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attemptCount++
		mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	cfg := Config{
		URLs: []string{server.URL},
	}
	svc := NewService(cfg)

	action := Action{
		Event:  EventMergeFinished,
		Status: StatusFailed,
	}

	err := svc.deliverWithRetry(context.Background(), server.URL, action)
	require.Error(t, err)

	mu.Lock()
	assert.Equal(t, 1, attemptCount, "Should not retry 4xx errors")
	mu.Unlock()
}

func TestDeliver_Retry429(t *testing.T) {
	attemptCount := 0
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attemptCount++
		currentAttempt := attemptCount
		mu.Unlock()

		if currentAttempt < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	cfg := Config{
		URLs: []string{server.URL},
	}
	svc := NewService(cfg)

	action := Action{
		Event:  EventMergeStarted,
		Status: StatusSuccess,
	}

	err := svc.deliverWithRetry(context.Background(), server.URL, action)
	require.NoError(t, err)

	mu.Lock()
	assert.Equal(t, 2, attemptCount, "Should retry rate limit errors")
	mu.Unlock()
}

func TestStart_NoURLs(t *testing.T) {
	cfg := Config{
		URLs: []string{},
	}
	svc := NewService(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Should return immediately without starting goroutine
	svc.Start(ctx)

	// Publish should still work (just goes to channel)
	svc.Publish(EventMergeStarted, StatusSuccess, nil)
}

func TestStart_MultipleURLs(t *testing.T) {
	receivedCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Create two test servers
	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		wg.Done()
		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedCount++
		mu.Unlock()
		wg.Done()
		w.WriteHeader(http.StatusOK)
	}))
	defer server2.Close()

	cfg := Config{
		URLs: []string{server1.URL, server2.URL},
	}
	svc := NewService(cfg)

	ctx := context.Background()
	svc.Start(ctx)

	wg.Add(2) // Expect both servers to receive
	svc.Publish(EventMergeStarted, StatusSuccess, map[string]any{"test": "data"})

	// Wait for both webhooks to be delivered
	done := make(chan bool)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		mu.Lock()
		assert.Equal(t, 2, receivedCount, "Both webhooks should have been delivered")
		mu.Unlock()
	case <-time.After(5 * time.Second):
		t.Fatal("Webhooks were not delivered in time")
	}
}

func TestStart_ContextCancellation(t *testing.T) {
	cfg := Config{
		URLs: []string{"http://example.com/webhook"},
	}
	svc := NewService(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	svc.Start(ctx)

	// Give it a moment to start
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	// Service should stop gracefully
	time.Sleep(100 * time.Millisecond)
}

func TestIsRetryableHTTPError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "500 error",
			err:      &httpError{statusCode: 500, url: "http://test"},
			expected: true,
		},
		{
			name:     "502 error",
			err:      &httpError{statusCode: 502, url: "http://test"},
			expected: true,
		},
		{
			name:     "503 error",
			err:      &httpError{statusCode: 503, url: "http://test"},
			expected: true,
		},
		{
			name:     "429 rate limit",
			err:      &httpError{statusCode: 429, url: "http://test"},
			expected: true,
		},
		{
			name:     "400 error",
			err:      &httpError{statusCode: 400, url: "http://test"},
			expected: false,
		},
		{
			name:     "404 error",
			err:      &httpError{statusCode: 404, url: "http://test"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableHTTPError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
