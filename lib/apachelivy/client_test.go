package apachelivy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestClientWithServer(handler http.Handler) (*Client, *httptest.Server) {
	server := httptest.NewServer(handler)
	client := &Client{
		url:        server.URL,
		httpClient: server.Client(),
		sessionID:  1,
	}
	return client, server
}

func TestCancelStatement(t *testing.T) {
	var cancelCalled atomic.Bool
	var capturedMethod, capturedPath atomic.Value

	client, server := newTestClientWithServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cancelCalled.Store(true)
		capturedMethod.Store(r.Method)
		capturedPath.Store(r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"msg": "cancelled"}`))
	}))
	defer server.Close()

	client.cancelStatement(42)
	assert.True(t, cancelCalled.Load())
	assert.Equal(t, "POST", capturedMethod.Load())
	assert.Equal(t, "/sessions/1/statements/42/cancel", capturedPath.Load())
}

func TestWaitForStatement_ContextCancellation(t *testing.T) {
	requestCount := atomic.Int32{}
	client, server := newTestClientWithServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":1,"state":"running","completed":0,"output":{"status":"ok"}}`))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := client.waitForStatement(ctx, 1)
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, 2*time.Second)
	assert.GreaterOrEqual(t, requestCount.Load(), int32(1))
}

func TestWaitForStatement_CompletedImmediately(t *testing.T) {
	client, server := newTestClientWithServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":1,"state":"available","completed":1,"output":{"status":"ok"}}`))
	}))
	defer server.Close()

	resp, err := client.waitForStatement(context.Background(), 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, resp.Completed)
}

func TestExecContext_CancelsStatementOnContextCancel(t *testing.T) {
	var cancelCalled atomic.Bool

	mux := http.NewServeMux()
	// Session endpoint: return idle session.
	mux.HandleFunc("/sessions/1", func(w http.ResponseWriter, r *http.Request) {
		resp := GetSessionResponse{ID: 1, State: StateIdle}
		data, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	// List sessions: return existing session.
	mux.HandleFunc("/sessions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			resp := CreateSessionResponse{ID: 1, State: StateIdle}
			data, _ := json.Marshal(resp)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write(data)
			return
		}
		resp := ListSessonResponse{Sessions: []GetSessionResponse{{ID: 1, State: StateIdle, Name: "test"}}}
		data, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	// Submit statement.
	mux.HandleFunc("/sessions/1/statements", func(w http.ResponseWriter, r *http.Request) {
		resp := CreateStatementResponse{ID: 99, State: "waiting"}
		data, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(data)
	})
	mux.HandleFunc("/sessions/1/statements/99", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":99,"state":"running","completed":0,"output":{"status":"ok"}}`))
	})
	// Cancel statement.
	mux.HandleFunc("/sessions/1/statements/99/cancel", func(w http.ResponseWriter, r *http.Request) {
		cancelCalled.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"msg": "cancelled"}`))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := &Client{
		url:         server.URL,
		httpClient:  server.Client(),
		sessionID:   1,
		sessionName: "test",
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := client.execContext(ctx, "SELECT 1", 0)
	assert.ErrorIs(t, err, context.Canceled)
	// Give cancelStatement (which uses a background context) time to complete.
	time.Sleep(100 * time.Millisecond)
	assert.True(t, cancelCalled.Load(), "cancelStatement should have been called")
}

func TestQueryContext_CancelsStatementOnContextCancel(t *testing.T) {
	var cancelCalled atomic.Bool

	mux := http.NewServeMux()
	mux.HandleFunc("/sessions/1", func(w http.ResponseWriter, r *http.Request) {
		resp := GetSessionResponse{ID: 1, State: StateIdle}
		data, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	mux.HandleFunc("/sessions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			resp := CreateSessionResponse{ID: 1, State: StateIdle}
			data, _ := json.Marshal(resp)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write(data)
			return
		}
		resp := ListSessonResponse{Sessions: []GetSessionResponse{{ID: 1, State: StateIdle, Name: "test"}}}
		data, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	})
	mux.HandleFunc("/sessions/1/statements", func(w http.ResponseWriter, r *http.Request) {
		resp := CreateStatementResponse{ID: 77, State: "waiting"}
		data, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(data)
	})
	mux.HandleFunc("/sessions/1/statements/77", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":77,"state":"running","completed":0,"output":{"status":"ok"}}`))
	})
	mux.HandleFunc("/sessions/1/statements/77/cancel", func(w http.ResponseWriter, r *http.Request) {
		cancelCalled.Store(true)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"msg": "cancelled"}`))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := &Client{
		url:         server.URL,
		httpClient:  server.Client(),
		sessionID:   1,
		sessionName: "test",
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := client.queryContext(ctx, "SELECT 1", 0)
	assert.ErrorIs(t, err, context.Canceled)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, cancelCalled.Load(), "cancelStatement should have been called")
}
