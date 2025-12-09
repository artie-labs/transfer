package snowflake

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	jsoniter "github.com/json-iterator/go"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/time/rate"

	"github.com/artie-labs/transfer/lib/batch"
	"github.com/artie-labs/transfer/lib/optimization"
)

// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-limitations#channel-limits
const maxChunkSize = 4 * 1024 * 1024 // 4MB

type SnowpipeStreamingChannel struct {
	mu                sync.Mutex
	continuationToken string
	RateLimiter       *rate.Limiter
}

func NewSnowpipeStreamingChannel() *SnowpipeStreamingChannel {
	return &SnowpipeStreamingChannel{
		mu:                sync.Mutex{},
		continuationToken: "",
		RateLimiter:       rate.NewLimiter(rate.Limit(10), 1),
	}
}

func (s *SnowpipeStreamingChannel) GetContinuationToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.continuationToken
}

func (s *SnowpipeStreamingChannel) UpdateContinuationToken(token string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.continuationToken = token
	return s.continuationToken
}

type SnowpipeStreamingChannelManager struct {
	mu          sync.Mutex
	maxChannels int
	config      *gosnowflake.Config

	channelNameToChannel map[string]*SnowpipeStreamingChannel

	ingestHost  string
	scopedToken string
	expiresAt   time.Time

	// refreshMutex ensures only one refresh operation happens at a time
	refreshMutex sync.Mutex
}

func NewSnowpipeStreamingChannelManager(config *gosnowflake.Config, maxChannels int) *SnowpipeStreamingChannelManager {
	return &SnowpipeStreamingChannelManager{
		config:               config,
		maxChannels:          maxChannels,
		channelNameToChannel: make(map[string]*SnowpipeStreamingChannel),
	}
}

func (s *SnowpipeStreamingChannelManager) refresh(ctx context.Context) error {
	// Fast path: check without lock first
	s.mu.Lock()
	needsRefresh := s.expiresAt.Before(time.Now().Add(1 * time.Minute))
	s.mu.Unlock()

	if !needsRefresh {
		return nil // No refresh needed
	}

	// Acquire refresh mutex to ensure only one refresh operation happens at a time
	s.refreshMutex.Lock()
	defer s.refreshMutex.Unlock()

	// Double-check: another goroutine may have refreshed while we were waiting
	s.mu.Lock()
	needsRefresh = s.expiresAt.Before(time.Now().Add(1 * time.Minute))
	s.mu.Unlock()

	if !needsRefresh {
		return nil // Another goroutine already refreshed
	}

	// Perform network operations (only one goroutine can be here at a time)
	jwt, err := PrepareJWTToken(s.config)
	if err != nil {
		return fmt.Errorf("failed to prepare JWT token: %w", err)
	}

	ingestHost, err := GetIngestHost(ctx, jwt, s.config.Account)
	if err != nil {
		return fmt.Errorf("failed to get ingest host: %w", err)
	}

	scopedToken, expiresAt, err := GetScopedToken(ctx, jwt, s.config.Account, ingestHost)
	if err != nil {
		return fmt.Errorf("failed to get scoped token: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.scopedToken = scopedToken
	s.expiresAt = expiresAt
	s.ingestHost = ingestHost

	return nil
}

func (s *SnowpipeStreamingChannelManager) LoadData(ctx context.Context, db, schema, pipe string, now time.Time, data optimization.TableData) error {
	s.mu.Lock()
	needsRefresh := s.expiresAt.Before(now.Add(1 * time.Minute))
	s.mu.Unlock()

	if needsRefresh {
		if err := s.refresh(ctx); err != nil {
			return fmt.Errorf("failed to refresh scoped token for snowpipe streaming: %w", err)
		}
	}

	channelName := fmt.Sprintf("%s-%d", data.Name(), 0)

	// Get or create channel and open it if needed, all while holding manager lock
	// This prevents multiple goroutines from calling OpenChannel() concurrently
	s.mu.Lock()
	channel, ok := s.channelNameToChannel[channelName]
	if !ok {
		channel = NewSnowpipeStreamingChannel()
		s.channelNameToChannel[channelName] = channel
	}

	// Check if channel needs to be opened
	if contToken := channel.GetContinuationToken(); contToken == "" {
		if channelResponse, err := OpenChannel(ctx, s.scopedToken, s.ingestHost, db, schema, pipe, channelName); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("failed to open channel for snowpipe streaming: %w", err)
		} else {
			channel.UpdateContinuationToken(channelResponse.NextContinuationToken)
		}
	}
	s.mu.Unlock()

	_, err := batch.BySize(
		data.Rows(),
		maxChunkSize,
		true,
		func(row optimization.Row) ([]byte, error) {
			rowBytes, err := jsoniter.Marshal(row.GetData())
			if err != nil {
				return nil, err
			}
			// Include newline in the encoded bytes - NDJSON
			return append(rowBytes, '\n'), nil
		},
		func(encodedBytes [][]byte, rows []optimization.Row) error {
			if err := channel.RateLimiter.Wait(ctx); err != nil {
				return fmt.Errorf("rate limiter error for channel %q: %w", channelName, err)
			}

			readers := make([]io.Reader, len(encodedBytes))
			for i, b := range encodedBytes {
				readers[i] = bytes.NewReader(b)
			}
			reader := io.MultiReader(readers...)

			appendResp, err := AppendRows(ctx, s.scopedToken, s.ingestHost, db, schema, pipe, channelName, channel.GetContinuationToken(), reader)
			if err != nil {
				return fmt.Errorf("failed to append rows for snowpipe streaming channel %q: %w", channelName, err)
			}

			channel.UpdateContinuationToken(appendResp.NextContinuationToken)
			return nil
		},
	)

	return err
}

// copied from https://github.com/snowflakedb/gosnowflake/blob/v1.17.0/auth.go#L640
func PrepareJWTToken(config *gosnowflake.Config) (string, error) {
	if config.PrivateKey == nil {
		return "", fmt.Errorf("trying to use keypair authentication, but PrivateKey was not provided in the driver config")
	}
	pubBytes, err := x509.MarshalPKIXPublicKey(config.PrivateKey.Public())
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(pubBytes)

	accountName := extractAccountName(config.Account)
	userName := strings.ToUpper(config.User)

	issueAtTime := time.Now().UTC()

	var timeout time.Duration
	if config.JWTExpireTimeout == 0 {
		timeout = 60 * time.Second
	} else {
		timeout = config.JWTExpireTimeout
	}

	jwtClaims := jwt.MapClaims{
		"iss": fmt.Sprintf("%s.%s.%s", accountName, userName, "SHA256:"+base64.StdEncoding.EncodeToString(hash[:])),
		"sub": fmt.Sprintf("%s.%s", accountName, userName),
		"iat": issueAtTime.Unix(),
		"nbf": time.Date(2015, 10, 10, 12, 0, 0, 0, time.UTC).Unix(),
		"exp": issueAtTime.Add(timeout).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwtClaims)

	tokenString, err := token.SignedString(config.PrivateKey)
	if err != nil {
		return "", err
	}

	return tokenString, err
}

func extractAccountName(rawAccount string) string {
	posDot := strings.Index(rawAccount, ".")
	if posDot > 0 {
		return strings.ToUpper(rawAccount[:posDot])
	}
	return strings.ToUpper(rawAccount)
}

func getControlHost(account string) string {
	return fmt.Sprintf("%s.snowflakecomputing.com", account)
}

// https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-tutorial#step-2-discover-ingest-host
func GetIngestHost(ctx context.Context, jwt, account string) (string, error) {
	controlHost := getControlHost(account)
	url := fmt.Sprintf("https://%s/v2/streaming/hostname", controlHost)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+jwt)
	req.Header.Set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("failed to make call to Snowflake hostname endpoint: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("unexpected status code from Snowflake hostname endpoint: %d, body: %s", resp.StatusCode, string(body))
	}

	ingestHostBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read body from Snowflake hostname endpoint: %w", err)
	}
	ingestHost := strings.ReplaceAll(string(ingestHostBytes), "_", "-")
	return ingestHost, nil
}

func GetScopedToken(ctx context.Context, jwtToken, account, ingestHost string) (scopedToken string, expiresAt time.Time, err error) {
	controlHost := getControlHost(account)
	url := fmt.Sprintf("https://%s/oauth/token", controlHost)
	data := fmt.Sprintf("grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&scope=%s", ingestHost)
	payload := strings.NewReader(data)

	req, err := http.NewRequestWithContext(ctx, "POST", url, payload)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer "+jwtToken)

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to make call to Snowflake oauth/token endpoint: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to read body from Snowflake oauth/token endpoint: %w", err)
	}
	if resp.StatusCode != 200 {
		return "", time.Time{}, fmt.Errorf("unexpected status code from Snowflake oauth/token endpoint: %d, body: %s", resp.StatusCode, string(body))
	}
	scopedToken = string(body)

	parser := jwt.Parser{}
	token, _, err := parser.ParseUnverified(scopedToken, jwt.MapClaims{})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to parse scoped token JWT: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return "", time.Time{}, fmt.Errorf("failed to extract claims from scoped token: claims are not MapClaims")
	}

	exp, ok := claims["exp"].(float64)
	if !ok {
		return "", time.Time{}, fmt.Errorf("failed to extract expiration time from scoped token: exp claim is missing or invalid type")
	}

	expiresAt = time.Unix(int64(exp), 0)
	return scopedToken, expiresAt, nil
}

type ChannelStatus struct {
	DatabaseName                    string    `json:"database_name"`
	SchemaName                      string    `json:"schema_name"`
	PipeName                        string    `json:"pipe_name"`
	ChannelName                     string    `json:"channel_name"`
	ChannelStatusCode               string    `json:"channel_status_code"`
	LastCommittedOffsetToken        string    `json:"last_committed_offset_token"`
	CreatedOnMs                     int64     `json:"created_on_ms"`
	RowsInserted                    int       `json:"rows_inserted"`
	RowsParsed                      int       `json:"rows_parsed"`
	RowsErrorCount                  int       `json:"rows_error_count"`
	LastErrorOffsetUpperBound       string    `json:"last_error_offset_upper_bound"`
	LastErrorMessage                string    `json:"last_error_message"`
	LastErrorTimestamp              time.Time `json:"last_error_timestamp"`
	SnowflakeAvgProcessingLatencyMs int       `json:"snowflake_avg_processing_latency_ms"`
}

type ChannelResponse struct {
	NextContinuationToken string        `json:"next_continuation_token"`
	ChannelStatus         ChannelStatus `json:"channel_status"`
}

func OpenChannel(ctx context.Context, scopedToken, ingestHost, db, schema, pipe, channelName string) (ChannelResponse, error) {
	url := fmt.Sprintf("https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s/channels/%s", ingestHost, db, schema, pipe, channelName)

	payload := strings.NewReader(`{}`)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, payload)
	if err != nil {
		return ChannelResponse{}, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+scopedToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ChannelResponse{}, fmt.Errorf("failed to perform PUT request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ChannelResponse{}, fmt.Errorf("failed to read response body: %w body: %s", err, string(body))
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return ChannelResponse{}, fmt.Errorf("unexpected status code %d, body: %s", resp.StatusCode, body)
	}

	var channelResponse ChannelResponse
	if err := json.Unmarshal(body, &channelResponse); err != nil {
		return ChannelResponse{}, fmt.Errorf("failed to unmarshal channel response: %w body: %s", err, string(body))
	}
	return channelResponse, nil
}

func GetChannelStatus(
	ctx context.Context,
	scopedToken, ingestHost, db, schema, pipe string,
	channels []string,
) (map[string]ChannelStatus, error) {
	url := fmt.Sprintf(
		"https://%s/v2/streaming/databases/%s/schemas/%s/pipes/%s:bulk-channel-status",
		ingestHost, db, schema, pipe,
	)

	type channelNamesPayload struct {
		ChannelNames []string `json:"channel_names"`
	}
	payloadBody, err := json.Marshal(channelNamesPayload{ChannelNames: channels})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bulk channel status payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payloadBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create POST request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+scopedToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to perform POST request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w body: %s", err, string(body))
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return nil, fmt.Errorf("unexpected status code %d, body: %s", resp.StatusCode, body)
	}

	var response struct {
		ChannelStatuses map[string]ChannelStatus `json:"channel_statuses"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal channel statuses: %w body: %s", err, string(body))
	}
	return response.ChannelStatuses, nil
}

type AppendRowsResponse struct {
	NextContinuationToken string `json:"next_continuation_token"`
}

func AppendRows(
	ctx context.Context,
	scopedToken, ingestHost, db, schema, pipe, channelName, contToken string,
	rowsData io.Reader,
) (AppendRowsResponse, error) {
	url := fmt.Sprintf(
		"https://%s/v2/streaming/data/databases/%s/schemas/%s/pipes/%s/channels/%s/rows?continuationToken=%s",
		ingestHost, db, schema, pipe, channelName, contToken,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, rowsData)
	if err != nil {
		return AppendRowsResponse{}, fmt.Errorf("failed to create POST request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+scopedToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return AppendRowsResponse{}, fmt.Errorf("failed to perform POST request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return AppendRowsResponse{}, fmt.Errorf("failed to read response body: %w body: %s", err, string(body))
	}

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return AppendRowsResponse{}, fmt.Errorf("unexpected status code %d, body: %s", resp.StatusCode, body)
	}

	var appendRowsResponse AppendRowsResponse
	if err := json.Unmarshal(body, &appendRowsResponse); err != nil {
		return AppendRowsResponse{}, fmt.Errorf("failed to unmarshal append rows response: %w body: %s", err, string(body))
	}
	return appendRowsResponse, nil
}
