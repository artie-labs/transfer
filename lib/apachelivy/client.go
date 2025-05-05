package apachelivy

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/retry"
)

const (
	sleepBaseMs                     = 1_000
	sleepMaxMs                      = 3_000
	defaultHeartbeatTimeoutInSecond = 300
	maxSessionRetries               = 5
)

type Client struct {
	mu                              sync.Mutex
	url                             string
	sessionID                       int
	httpClient                      *http.Client
	sessionConf                     map[string]any
	sessionJars                     []string
	sessionHeartbeatTimeoutInSecond int
	sessionDriverMemory             string
	sessionExecutorMemory           string

	lastChecked time.Time
}

const sessionBufferSeconds = 30

func shouldCreateNewSession(resp GetSessionResponse, statusCode int, err error) (bool, error) {
	if statusCode == http.StatusNotFound {
		return true, nil
	}

	if err != nil {
		return false, err
	}

	// If the session is in a terminal state, then we should create a new one.
	return slices.Contains(TerminalSessionStates, resp.State), nil
}

func (c *Client) ensureSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sessionID == 0 {
		c.lastChecked = time.Now()
		return c.newSession(ctx, SessionKindSql, true)
	}

	if time.Since(c.lastChecked).Seconds() > (float64(c.sessionHeartbeatTimeoutInSecond) - sessionBufferSeconds) {
		c.lastChecked = time.Now()
		shouldCreateNewSession, err := shouldCreateNewSession(c.getSession(ctx))
		if err != nil {
			return err
		}

		if shouldCreateNewSession {
			return c.newSession(ctx, SessionKindSql, true)
		}
	}

	return nil
}

func (c *Client) buildRetryConfig() (retry.RetryConfig, error) {
	// TODO: Move this from [retry.AlwaysRetry] to be more targeted
	cfg, err := retry.NewJitterRetryConfig(sleepBaseMs, sleepMaxMs, maxSessionRetries, retry.AlwaysRetry)
	if err != nil {
		return nil, fmt.Errorf("failed to create retry config: %w", err)
	}

	return cfg, nil
}

func (c *Client) queryContext(ctx context.Context, query string) (GetStatementResponse, error) {
	if err := c.ensureSession(ctx); err != nil {
		return GetStatementResponse{}, err
	}

	statementID, err := c.submitLivyStatement(ctx, query)
	if err != nil {
		return GetStatementResponse{}, err
	}

	response, err := c.waitForStatement(ctx, statementID)
	if err != nil {
		return GetStatementResponse{}, err
	}

	return response, nil
}

func (c *Client) QueryContext(ctx context.Context, query string) (GetStatementResponse, error) {
	retryCfg, err := c.buildRetryConfig()
	if err != nil {
		return GetStatementResponse{}, err
	}

	return retry.WithRetriesAndResult(retryCfg, func(_ int, _ error) (GetStatementResponse, error) {
		return c.queryContext(ctx, query)
	})
}

func (c *Client) execContext(ctx context.Context, query string) error {
	if err := c.ensureSession(ctx); err != nil {
		return err
	}

	statementID, err := c.submitLivyStatement(ctx, query)
	if err != nil {
		return err
	}

	resp, err := c.waitForStatement(ctx, statementID)
	if err != nil {
		return err
	}

	return resp.Error(c.sessionID)
}

func (c *Client) ExecContext(ctx context.Context, query string) error {
	retryCfg, err := c.buildRetryConfig()
	if err != nil {
		return err
	}

	return retry.WithRetries(retryCfg, func(_ int, _ error) error {
		return c.execContext(ctx, query)
	})
}

func (c *Client) waitForStatement(ctx context.Context, statementID int) (GetStatementResponse, error) {
	var count int
	for {
		out, err := c.doRequest(ctx, "GET", fmt.Sprintf("/sessions/%d/statements/%d", c.sessionID, statementID), nil)
		if err != nil {
			return GetStatementResponse{}, err
		}

		var resp GetStatementResponse
		if err := json.Unmarshal(out.body, &resp); err != nil {
			return GetStatementResponse{}, err
		}

		if resp.Completed > 0 {
			// Response finished, so let's see if the response is an error or not.
			if err := resp.Error(c.sessionID); err != nil {
				return GetStatementResponse{}, err
			}

			return resp, nil
		}

		// It's not ready yet, so we're going to sleep a bit and check again.
		sleepTime := jitter.Jitter(sleepBaseMs, sleepMaxMs, count)
		slog.Info("Statement is not ready yet, sleeping", slog.Duration("sleepTime", sleepTime))
		time.Sleep(sleepTime)
		count++
	}
}

func (c *Client) submitLivyStatement(ctx context.Context, code string) (int, error) {
	reqBody, err := json.Marshal(CreateStatementRequest{Code: code, Kind: "sql"})
	if err != nil {
		return 0, err
	}

	out, err := c.doRequest(ctx, "POST", fmt.Sprintf("/sessions/%d/statements", c.sessionID), reqBody)
	if err != nil {
		return 0, err
	}

	var resp CreateStatementResponse
	if err := json.Unmarshal(out.body, &resp); err != nil {
		return 0, err
	}

	return resp.ID, nil
}

type doRequestResponse struct {
	body       []byte
	httpStatus int
}

func (c *Client) doRequest(ctx context.Context, method, path string, body []byte) (doRequestResponse, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.url+path, bytes.NewBuffer(body))
	if err != nil {
		return doRequestResponse{}, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return doRequestResponse{}, err
	}

	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return doRequestResponse{}, err
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return doRequestResponse{body: out, httpStatus: resp.StatusCode}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return doRequestResponse{body: out, httpStatus: resp.StatusCode}, nil
}

func (c *Client) newSession(ctx context.Context, kind SessionKind, blockUntilReady bool) error {
	request := CreateSessionRequest{
		Kind:                     string(kind),
		Jars:                     c.sessionJars,
		Conf:                     c.sessionConf,
		HeartbeatTimeoutInSecond: c.sessionHeartbeatTimeoutInSecond,
		DriverMemory:             c.sessionDriverMemory,
		ExecutorMemory:           c.sessionExecutorMemory,
	}

	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	resp, err := c.doRequest(ctx, "POST", "/sessions", body)
	if err != nil {
		var errorResponse ErrorResponse
		if err = json.Unmarshal(resp.body, &errorResponse); err != nil {
			return fmt.Errorf("failed to unmarshal error response: %w", err)
		}

		if errorResponse.Message == ErrTooManySessionsCreated {
			sleepTime := jitter.Jitter(sleepBaseMs, sleepMaxMs, 0)
			slog.Info("Too many sessions created, throttling", slog.String("message", errorResponse.Message), slog.Duration("sleepTime", sleepTime))
			time.Sleep(sleepTime)
			return c.newSession(ctx, kind, blockUntilReady)
		}

		slog.Warn("Failed to create session", slog.Any("err", err), slog.String("response", string(resp.body)))
		return err
	}

	var createResp CreateSessionResponse
	if err = json.Unmarshal(resp.body, &createResp); err != nil {
		return err
	}

	c.sessionID = createResp.ID
	if blockUntilReady && !createResp.State.IsReady() {
		if err := c.waitForSessionToBeReady(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) getSession(ctx context.Context) (GetSessionResponse, int, error) {
	out, err := c.doRequest(ctx, "GET", fmt.Sprintf("/sessions/%d", c.sessionID), nil)
	if err != nil {
		return GetSessionResponse{}, out.httpStatus, err
	}

	var resp GetSessionResponse
	if err := json.Unmarshal(out.body, &resp); err != nil {
		return GetSessionResponse{}, out.httpStatus, err
	}

	return resp, out.httpStatus, nil
}

func (c *Client) waitForSessionToBeReady(ctx context.Context) error {
	var count int
	for {
		resp, _, err := c.getSession(ctx)
		if err != nil {
			return err
		}

		switch resp.State {
		case StateIdle:
			return nil
		case StateNotStarted, StateStarting:
			sleepTime := jitter.Jitter(sleepBaseMs, sleepMaxMs, count)
			slog.Info("Session is not ready yet, sleeping", slog.Int("sessionID", c.sessionID), slog.Int("count", count), slog.Duration("sleepTime", sleepTime))
			time.Sleep(sleepTime)
		default:
			return fmt.Errorf("session in unexpected state: %q", resp.State)
		}

		count++
	}
}

func (c *Client) ListSessions(ctx context.Context) (ListSessonResponse, error) {
	out, err := c.doRequest(ctx, "GET", "/sessions", nil)
	if err != nil {
		return ListSessonResponse{}, err
	}

	var resp ListSessonResponse
	if err := json.Unmarshal(out.body, &resp); err != nil {
		return ListSessonResponse{}, err
	}

	return resp, nil
}

func (c *Client) DeleteSession(ctx context.Context, sessionID int) error {
	if _, err := c.doRequest(ctx, http.MethodDelete, fmt.Sprintf("/sessions/%d", sessionID), nil); err != nil {
		return err
	}

	return nil
}

func NewClient(ctx context.Context, url string, config map[string]any, jars []string, heartbeatTimeoutInSecond int, driverMemory, executorMemory string) (*Client, error) {
	client := &Client{
		url:                             url,
		httpClient:                      &http.Client{},
		sessionConf:                     config,
		sessionJars:                     jars,
		sessionHeartbeatTimeoutInSecond: cmp.Or(heartbeatTimeoutInSecond, defaultHeartbeatTimeoutInSecond),
		sessionDriverMemory:             driverMemory,
		sessionExecutorMemory:           executorMemory,
	}

	return client, nil
}
