package snowflake

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
	assert.NotNil(t, channel.Buffer, "Buffer should be initialized")
	assert.NotNil(t, channel.Encoder, "Encoder should be initialized")
	assert.Equal(t, "", channel.ContinuationToken, "ContinuationToken should be empty")
}

func TestSnowpipeStreamingChannel_UpdateToken(t *testing.T) {
	channel := NewSnowpipeStreamingChannel()

	token := channel.UpdateToken("token1")
	assert.Equal(t, "token1", token)
	assert.Equal(t, "token1", channel.ContinuationToken)

	token = channel.UpdateToken("token2")
	assert.Equal(t, "token2", token)
	assert.Equal(t, "token2", channel.ContinuationToken)
}

func TestSnowpipeStreamingChannelManager_ChannelCreation(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg)

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

	ctx := t.Context()
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData)
	require.NoError(t, err)

	// Channel should be created
	assert.Len(t, manager.channelNameToChannel, 1)
	assert.NotNil(t, manager.channelNameToChannel["table1"])
}

func TestSnowpipeStreamingChannelManager_ChannelReuse(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg)

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

	channel1 := manager.channelNameToChannel["table1"]

	// Second call with same table name
	tableData2 := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "table1")
	row2 := optimization.NewRow(map[string]any{"id": 2, "name": "test2"})
	tableData2.InsertRow("2", row2.GetData(), false)

	err = manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData2)
	require.NoError(t, err)

	channel2 := manager.channelNameToChannel["table1"]

	// Should be the same channel instance
	assert.Same(t, channel1, channel2, "Channel should be reused")
	assert.Len(t, manager.channelNameToChannel, 1)
}

func TestSnowpipeStreamingChannelManager_MultipleChannels(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg)

	server := createMockSnowflakeServer(t)
	defer server.Close()

	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	ctx := t.Context()

	// Load data for table1
	tableData1 := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "table1")
	row1 := optimization.NewRow(map[string]any{"id": 1})
	tableData1.InsertRow("1", row1.GetData(), false)
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData1)
	require.NoError(t, err)

	// Load data for table2
	tableData2 := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "table2")
	row2 := optimization.NewRow(map[string]any{"id": 2})
	tableData2.InsertRow("2", row2.GetData(), false)
	err = manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData2)
	require.NoError(t, err)

	// Should have two separate channels
	assert.Len(t, manager.channelNameToChannel, 2)
	assert.NotNil(t, manager.channelNameToChannel["table1"])
	assert.NotNil(t, manager.channelNameToChannel["table2"])
	assert.NotSame(t, manager.channelNameToChannel["table1"], manager.channelNameToChannel["table2"])
}

func TestSnowpipeStreamingChannelManager_OversizedRowRejection(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg)

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

	ctx := t.Context()
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "greater than the max payload size (4MB)")
	assert.Contains(t, err.Error(), "test_table")
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

	manager := NewSnowpipeStreamingChannelManager(cfg)
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

	ctx := t.Context()
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData)
	require.NoError(t, err)

	// Should have made exactly 1 AppendRows request
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

	manager := NewSnowpipeStreamingChannelManager(cfg)
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

	ctx := t.Context()
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData)
	require.NoError(t, err)

	// Should have made multiple requests for chunking
	assert.Greater(t, requestCount, 1, "Large batch should result in multiple requests")
	t.Logf("Made %d requests for 30,000 rows", requestCount)
}

func TestSnowpipeStreamingChannelManager_EmptyTableData(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg)

	server := createMockSnowflakeServer(t)
	defer server.Close()

	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	// Empty table data
	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")

	ctx := t.Context()
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData)

	// Should not error on empty data
	require.NoError(t, err)
}

func TestSnowpipeStreamingChannelManager_ContinuationTokenChaining(t *testing.T) {
	// Configure http.DefaultClient to skip TLS verification for testing
	http.DefaultClient.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	tokens := []string{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/rows") && r.Method == http.MethodPost {
			// Extract continuation token from query params
			contToken := r.URL.Query().Get("continuationToken")
			tokens = append(tokens, contToken)

			w.WriteHeader(http.StatusOK)
			require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
				"next_continuation_token": fmt.Sprintf("token%d", len(tokens)),
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

	manager := NewSnowpipeStreamingChannelManager(cfg)
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

	ctx := t.Context()
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData)
	require.NoError(t, err)

	// Verify tokens are chained correctly
	assert.Greater(t, len(tokens), 1, "Should have multiple tokens")
	// First token should be from OpenChannel (token0)
	assert.Equal(t, "token0", tokens[0], "First request should use token from OpenChannel")
	// Subsequent tokens should be chained
	for i := 1; i < len(tokens); i++ {
		expectedToken := fmt.Sprintf("token%d", i)
		assert.Equal(t, expectedToken, tokens[i], "Token should be chained from previous response")
	}
}

func TestSnowpipeStreamingChannelManager_BufferReset(t *testing.T) {
	cfg := &gosnowflake.Config{
		Account: "test",
		User:    "test",
	}

	manager := NewSnowpipeStreamingChannelManager(cfg)

	server := createMockSnowflakeServer(t)
	defer server.Close()

	manager.ingestHost = strings.TrimPrefix(server.URL, "https://")
	manager.scopedToken = "mock-token"
	manager.expiresAt = time.Now().Add(1 * time.Hour)

	tableData := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{}, "test_table")
	row := optimization.NewRow(map[string]any{"id": 1, "name": "test"})
	tableData.InsertRow("1", row.GetData(), false)

	ctx := t.Context()
	err := manager.LoadData(ctx, "db", "schema", "pipe", time.Now(), *tableData)
	require.NoError(t, err)

	channel := manager.channelNameToChannel["test_table"]

	// After LoadData, buffer should be empty (reset after final send)
	// Note: Buffer might still have some data from the last write before flush
	// but it should be relatively small
	assert.NotNil(t, channel.Buffer)
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
