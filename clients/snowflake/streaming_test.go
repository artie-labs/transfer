package snowflake

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
)

func TestSnowpipeStreamingChannel_RateLimiterInitialization(t *testing.T) {
	channel := NewSnowpipeStreamingChannel()
	assert.NotNil(t, channel.RateLimiter, "RateLimiter should be initialized")
	assert.Equal(t, "", channel.GetContinuationToken(), "ContinuationToken should be empty")
}

func TestSnowpipeStreamingChannel_InvalidateChannel(t *testing.T) {
	channel := NewSnowpipeStreamingChannel()

	// Set a token first
	channel.UpdateContinuationToken("test-token")
	assert.Equal(t, "test-token", channel.GetContinuationToken())

	// Invalidate should clear the token
	channel.InvalidateChannel()
	assert.Equal(t, "", channel.GetContinuationToken())
}

func TestParseChannelError(t *testing.T) {
	{
		// STALE_PIPE_CACHE error
		result := NewErrorResponse("STALE_PIPE_CACHE", "A pipe with this name was recently dropped")
		assert.True(t, result.IsChannelReopenError())
		assert.Equal(t, "STALE_PIPE_CACHE", result.Code)
		assert.Equal(t, "A pipe with this name was recently dropped", result.Message)
	}
	{
		// ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED error
		result := NewErrorResponse("ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED", "")
		assert.True(t, result.IsChannelReopenError())
		assert.Equal(t, "ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED", result.Code)
		assert.Equal(t, "", result.Message)
	}
	{
		// ERR_CHANNEL_MUST_BE_REOPENED error
		result := NewErrorResponse("ERR_CHANNEL_MUST_BE_REOPENED", "Channel must be reopened")
		assert.True(t, result.IsChannelReopenError())
		assert.Equal(t, "ERR_CHANNEL_MUST_BE_REOPENED", result.Code)
		assert.Equal(t, "Channel must be reopened", result.Message)
	}
	{
		// ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER error
		result := NewErrorResponse("ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER", "")
		assert.True(t, result.IsChannelReopenError())
		assert.Equal(t, "ERR_CHANNEL_HAS_INVALID_ROW_SEQUENCER", result.Code)
	}
	{
		// ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER error
		result := NewErrorResponse("ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER", "")
		assert.True(t, result.IsChannelReopenError())
		assert.Equal(t, "ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER", result.Code)
	}
	{
		// ERR_CHANNEL_MUST_BE_REOPENED_DUE_TO_ROW_SEQ_GAP error
		result := NewErrorResponse("ERR_CHANNEL_MUST_BE_REOPENED_DUE_TO_ROW_SEQ_GAP", "")
		assert.True(t, result.IsChannelReopenError())
		assert.Equal(t, "ERR_CHANNEL_MUST_BE_REOPENED_DUE_TO_ROW_SEQ_GAP", result.Code)
	}
	{
		// STALE_CONTINUATION_TOKEN_SEQUENCER error
		result := NewErrorResponse("STALE_CONTINUATION_TOKEN_SEQUENCER", "Channel sequencer in the continuation token is stale. Please reopen the channel")
		assert.True(t, result.IsChannelReopenError())
		assert.Equal(t, "STALE_CONTINUATION_TOKEN_SEQUENCER", result.Code)
	}
	{
		// Non-reopenable error
		result := NewErrorResponse("SOME_OTHER_ERROR", "Something else went wrong")
		assert.False(t, result.IsChannelReopenError())
	}
}

func TestChannelReopenableError_Error(t *testing.T) {
	{
		// With message
		err := NewErrorResponse("STALE_PIPE_CACHE", "A pipe with this name was recently dropped. The server cache has been updated. Please reopen the channel and try again")
		assert.Equal(t, "STALE_PIPE_CACHE: A pipe with this name was recently dropped. The server cache has been updated. Please reopen the channel and try again", err.Error())
	}
	{
		// Without message
		err := NewErrorResponse("ERR_CHANNEL_MUST_BE_REOPENED", "")
		assert.Equal(t, "ERR_CHANNEL_MUST_BE_REOPENED", err.Error())
	}
}

func TestSnowpipeStreamingChannel_UpdateToken(t *testing.T) {
	channel := NewSnowpipeStreamingChannel()

	token := channel.UpdateContinuationToken("token1")
	assert.Equal(t, "token1", token)
	assert.Equal(t, "token1", channel.GetContinuationToken())

	token = channel.UpdateContinuationToken("token2")
	assert.Equal(t, "token2", token)
	assert.Equal(t, "token2", channel.GetContinuationToken())
}

func TestSnowpipeStreamingChannelManager_ChannelCreation(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)

	// Initially no channels
	assert.Len(t, manager.channelNameToChannel, 0)

	// Mock server for API calls
	server := createMockSnowflakeServer(t)
	defer server.Close()

	// Override the ingest host to use our mock server
	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "table1")
	row := optimization.NewRow(map[string]any{"id": 1, "name": "test"})
	tableData.InsertRow("1", row.GetData(), false)

	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData))

	// Channel should be created
	assert.Len(t, manager.channelNameToChannel, 1)
	assert.NotNil(t, manager.channelNameToChannel["table1-0"])
}

func TestSnowpipeStreamingChannelManager_ChannelReuse(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)

	// Mock server
	server := createMockSnowflakeServer(t)
	defer server.Close()

	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	// First call
	tableData1 := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "table1")

	row1 := optimization.NewRow(map[string]any{"id": 1, "name": "test1"})
	tableData1.InsertRow("1", row1.GetData(), false)

	ctx := t.Context()
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData1)
	require.NoError(t, err)

	channel1 := manager.channelNameToChannel["table1-0"]

	// Second call with same table name
	tableData2 := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "table1")

	row2 := optimization.NewRow(map[string]any{"id": 2, "name": "test2"})
	tableData2.InsertRow("2", row2.GetData(), false)

	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData2))

	channel2 := manager.channelNameToChannel["table1-0"]

	// Should be the same channel instance
	assert.Same(t, channel1, channel2, "Channel should be reused")
	assert.Len(t, manager.channelNameToChannel, 1)
}

func TestSnowpipeStreamingChannelManager_MultipleChannels(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)

	server := createMockSnowflakeServer(t)
	defer server.Close()

	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	// Load data for table1
	tableData1 := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "table1")

	row1 := optimization.NewRow(map[string]any{"id": 1})
	tableData1.InsertRow("1", row1.GetData(), false)
	err := manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData1)
	require.NoError(t, err)

	// Load data for table2
	tableData2 := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "table2")

	row2 := optimization.NewRow(map[string]any{"id": 2})
	tableData2.InsertRow("2", row2.GetData(), false)
	require.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData2))

	// Should have two separate channels
	assert.Len(t, manager.channelNameToChannel, 2)
	assert.NotNil(t, manager.channelNameToChannel["table1-0"])
	assert.NotNil(t, manager.channelNameToChannel["table2-0"])
	assert.NotSame(t, manager.channelNameToChannel["table1-0"], manager.channelNameToChannel["table2-0"])
}

func TestSnowpipeStreamingChannelManager_OversizedRowRejection(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)

	server := createMockSnowflakeServer(t)
	defer server.Close()

	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")

	// Create a row larger than 4MB
	largeData := strings.Repeat("x", 5*1024*1024) // 5MB
	row := optimization.NewRow(map[string]any{
		"id":   1,
		"data": largeData,
	})

	tableData.InsertRow("1", row.GetData(), false)
	require.ErrorContains(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData), "is larger")
}

func TestSnowpipeStreamingChannelManager_SmallBatchSingleRequest(t *testing.T) {
	// Configure http.DefaultClient to skip TLS verification for testing
	http.DefaultClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	requestCount := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost {
			requestCount++
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": fmt.Sprintf("token%d", requestCount),
			}))
		} else if strings.Contains(r.URL.Path, "/channels/") && r.Method == http.MethodPut {
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "initial-token",
			}))
		}
	}))
	defer server.Close()

	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)
	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	// Small batch - should result in single request
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")
	for i := 0; i < 10; i++ {
		row := optimization.NewRow(map[string]any{
			"id":   i,
			"name": fmt.Sprintf("user%d", i),
		})
		tableData.InsertRow(fmt.Sprintf("%d", i), row.GetData(), false)
	}

	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData))
	assert.Equal(t, 1, requestCount, "Small batch should result in single request")
}

func TestSnowpipeStreamingChannelManager_LargeBatchMultipleRequests(t *testing.T) {
	// Configure http.DefaultClient to skip TLS verification for testing
	http.DefaultClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	requestCount := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost {
			requestCount++

			// Read and verify the body size
			body, _ := io.ReadAll(r.Body)
			assert.LessOrEqual(t, len(body), maxChunkSize, "Each request should be <= 4MB")

			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": fmt.Sprintf("token%d", requestCount),
			}))
		} else if strings.Contains(r.URL.Path, "/channels/") && r.Method == http.MethodPut {
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "initial-token",
			}))
		}
	}))
	defer server.Close()

	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)
	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	// Large batch that should exceed 4MB
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")
	for i := 0; i < 30000; i++ {
		row := optimization.NewRow(map[string]any{
			"id":          i,
			"name":        fmt.Sprintf("user%d", i),
			"email":       fmt.Sprintf("user%d@example.com", i),
			"description": strings.Repeat("x", 150), // ~150 bytes per row
		})
		tableData.InsertRow(fmt.Sprintf("%d", i), row.GetData(), false)
	}

	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData))

	// Should have made multiple requests for chunking
	assert.Greater(t, requestCount, 1, "Large batch should result in multiple requests")
	t.Logf("Made %d requests for 30,000 rows", requestCount)
}

func TestSnowpipeStreamingChannelManager_EmptyTableData(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)

	server := createMockSnowflakeServer(t)
	defer server.Close()

	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	// Empty table data
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")
	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData))
}

func TestSnowpipeStreamingChannelManager_ContinuationTokenChaining(t *testing.T) {
	// Configure http.DefaultClient to skip TLS verification for testing
	http.DefaultClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	var mu sync.Mutex
	requestCount := 0
	expectedToken := "token0" // Start with token from OpenChannel

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost {
			mu.Lock()
			requestCount++
			// Verify the request uses the expected continuation token
			actualToken := r.URL.Query().Get("continuationToken")
			assert.Equal(t, expectedToken, actualToken, "Request should use token from previous response")
			// Set next expected token
			expectedToken = fmt.Sprintf("token%d", requestCount)
			nextToken := expectedToken
			mu.Unlock()

			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": nextToken,
			}))
		} else if strings.Contains(r.URL.Path, "/channels/") && r.Method == http.MethodPut {
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "token0",
			}))
		}
	}))
	defer server.Close()

	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)
	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	// Create data that will trigger multiple chunks
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")
	for i := 0; i < 25000; i++ {
		row := optimization.NewRow(map[string]any{
			"id":   i,
			"data": strings.Repeat("x", 200),
		})
		tableData.InsertRow(fmt.Sprintf("%d", i), row.GetData(), false)
	}

	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData))

	// Verify multiple chunks were sent
	mu.Lock()
	finalCount := requestCount
	mu.Unlock()

	assert.Greater(t, finalCount, 1, "Should have made multiple requests for chunking")
}

func TestSnowpipeStreamingChannelManager_SingleRowBatch(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)

	server := createMockSnowflakeServer(t)
	defer server.Close()

	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")

	row := optimization.NewRow(map[string]any{"id": 1, "name": "test"})
	tableData.InsertRow("1", row.GetData(), false)

	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData))

	// Verify channel was created and has updated continuation token
	channel := manager.channelNameToChannel["test_table-0"]
	assert.NotEmpty(t, channel.GetContinuationToken())
}

func TestSnowpipeStreamingChannelManager_ChannelReopenOnError(t *testing.T) {
	http.DefaultClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	var mu sync.Mutex
	appendCallCount := 0
	openChannelCallCount := 0

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if strings.Contains(r.URL.Path, "/channels/") && r.Method == http.MethodPut {
			openChannelCallCount++
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": fmt.Sprintf("token-open-%d", openChannelCallCount),
			}))
			return
		}

		if strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost {
			appendCallCount++
			if appendCallCount == 1 {
				// First call fails with STALE_PIPE_CACHE
				w.WriteHeader(http.StatusServiceUnavailable)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"code":    "STALE_PIPE_CACHE",
					"message": "A pipe with this name was recently dropped",
				}))
				return
			}
			// Second call succeeds
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "token-after-retry",
			}))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)
	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")

	row := optimization.NewRow(map[string]any{"id": 1, "name": "test"})
	tableData.InsertRow("1", row.GetData(), false)
	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData))

	mu.Lock()
	defer mu.Unlock()
	// Should have opened channel twice (initial + reopen after error)
	assert.Equal(t, 2, openChannelCallCount, "Channel should be opened twice (initial + reopen)")
	// Should have called append twice (first fails, retry succeeds)
	assert.Equal(t, 2, appendCallCount, "Append should be called twice (fail + retry)")
}

func TestSnowpipeStreamingChannelManager_NonReopenableErrorNoRetry(t *testing.T) {
	http.DefaultClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	var mu sync.Mutex
	appendCallCount := 0
	openChannelCallCount := 0

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if strings.Contains(r.URL.Path, "/channels/") && r.Method == http.MethodPut {
			openChannelCallCount++
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "initial-token",
			}))
			return
		}

		if strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost {
			appendCallCount++
			// Return a non-reopenable error
			w.WriteHeader(http.StatusBadRequest)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"code":    "INVALID_DATA_FORMAT",
				"message": "Data format is invalid",
			}))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)
	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")

	pk := "1"
	row := optimization.NewRow(map[string]any{"id": 1, "name": "test"})
	tableData.InsertRow(pk, row.GetData(), false)
	assert.ErrorContains(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData), "failed to append rows")

	mu.Lock()
	defer mu.Unlock()
	// Should have only opened channel once (no reopen for non-reopenable errors)
	assert.Equal(t, 1, openChannelCallCount, "Channel should only be opened once")
	// Should have only called append once (no retry for non-reopenable errors)
	assert.Equal(t, 1, appendCallCount, "Append should only be called once")
}

func TestSnowpipeStreamingChannelManager_ChannelReopenOnERR_CHANNEL_DOES_NOT_EXIST(t *testing.T) {
	http.DefaultClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	var mu sync.Mutex
	appendCallCount := 0
	openChannelCallCount := 0

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if strings.Contains(r.URL.Path, "/channels/") && r.Method == http.MethodPut {
			openChannelCallCount++
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": fmt.Sprintf("token-%d", openChannelCallCount),
			}))
			return
		}

		if strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost {
			appendCallCount++
			if appendCallCount == 1 {
				// First call fails with ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED
				w.WriteHeader(http.StatusBadRequest)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"code":    "ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED",
					"message": "",
				}))
				return
			}
			// Retry succeeds
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "success-token",
			}))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg, 1)
	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")

	pk := "1"
	row := optimization.NewRow(map[string]any{"id": 1, "name": "test"})
	tableData.InsertRow(pk, row.GetData(), false)

	assert.NoError(t, manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, openChannelCallCount, "Channel should be reopened after ERR_CHANNEL_DOES_NOT_EXIST")
	assert.Equal(t, 2, appendCallCount, "Append should be retried after channel reopen")
}

type appendTestResult struct {
	err              error
	openChannelCount int
	appendCount      int
	manager          *SnowpipeStreamingChannelManager
}

// runAppendTest sets up a mock server where sequential append calls return the given error codes.
// An empty string or calls beyond the slice length return success.
func runAppendTest(t *testing.T, errorCodes ...string) appendTestResult {
	t.Helper()
	http.DefaultClient.Transport = &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}

	var mu sync.Mutex
	var result appendTestResult

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if strings.Contains(r.URL.Path, "/channels/") && r.Method == http.MethodPut {
			result.openChannelCount++
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": fmt.Sprintf("token-open-%d", result.openChannelCount),
			}))
			return
		}

		if strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost {
			result.appendCount++
			idx := result.appendCount - 1
			if idx < len(errorCodes) && errorCodes[idx] != "" {
				w.WriteHeader(http.StatusBadRequest)
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
					"code":    errorCodes[idx],
					"message": "mock error",
				}))
				return
			}

			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": fmt.Sprintf("token-%d", result.appendCount),
			}))
			return
		}

		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	result.manager = NewSnowpipeStreamingChannelManager(&gosnowflake.Config{Account: "test", User: "test"}, 1)
	result.manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	result.manager.scopedToken = "mock-token"
	result.manager.expiresAt = time.Now().Add(1 * time.Hour)

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")
	tableData.InsertRow("1", map[string]any{"id": 1, "name": "test"}, false)

	result.err = result.manager.LoadData(t.Context(), "db", "schema", "pipe", time.Now(), *tableData)
	return result
}

func TestSnowpipeStreamingChannelManager_AppendErrorHandling(t *testing.T) {
	{
		// DUPLICATE_ROWSET treated as success — reopen to refresh token, no retry
		result := runAppendTest(t, "DUPLICATE_ROWSET")
		assert.NoError(t, result.err)
		assert.Equal(t, 2, result.openChannelCount)
		assert.Equal(t, 1, result.appendCount)
	}
	{
		// STALE_CONTINUATION_TOKEN_SEQUENCER triggers reopen and retry
		result := runAppendTest(t, "STALE_CONTINUATION_TOKEN_SEQUENCER")
		assert.NoError(t, result.err)
		assert.Equal(t, 2, result.openChannelCount)
		assert.Equal(t, 2, result.appendCount)
	}
	{
		// Multiple reopen retries before success
		result := runAppendTest(t, "STALE_PIPE_CACHE", "STALE_PIPE_CACHE")
		assert.NoError(t, result.err)
		assert.Equal(t, 3, result.openChannelCount)
		assert.Equal(t, 3, result.appendCount)
	}
	{
		// Reopen retries exhausted — error returned, channels invalidated
		result := runAppendTest(t, "STALE_PIPE_CACHE", "STALE_PIPE_CACHE", "STALE_PIPE_CACHE")
		assert.ErrorContains(t, result.err, fmt.Sprintf("exhausted %d attempts", maxAppendAttempts))
		assert.Equal(t, 1+maxAppendAttempts, result.openChannelCount)
		assert.Equal(t, maxAppendAttempts, result.appendCount)
		assert.Equal(t, "", result.manager.channelNameToChannel["test_table-0"].GetContinuationToken())
	}
}

// Helper function to create a mock Snowflake TLS server
func createMockSnowflakeServer(t *testing.T) *httptest.Server {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/channels/") && r.Method == http.MethodPut:
			// OpenChannel
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "initial-token",
				"channel_status": map[string]any{
					"channel_status_code": "ACTIVE",
				},
			}))
		case strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost:
			// AppendRows
			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": "next-token",
			}))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	// Configure http.DefaultClient to skip TLS verification for testing
	http.DefaultClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	return server
}
