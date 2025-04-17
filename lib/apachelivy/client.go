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
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
)

const (
	sleepBaseMs                     = 1_000
	sleepMaxMs                      = 3_000
	defaultHeartbeatTimeoutInSecond = 300
)

type Client struct {
	url                             string
	sessionID                       int
	httpClient                      *http.Client
	sessionConf                     map[string]any
	sessionJars                     []string
	sessionHeartbeatTimeoutInSecond int

	lastChecked time.Time
}

const sessionBufferSeconds = 30

func (c *Client) ensureSession(ctx context.Context) error {
	if c.sessionID == 0 {
		c.lastChecked = time.Now()
		return c.newSession(ctx, SessionKindSql, true)
	}

	if time.Since(c.lastChecked).Seconds() > (float64(c.sessionHeartbeatTimeoutInSecond) - sessionBufferSeconds) {
		c.lastChecked = time.Now()
		out, err := c.doRequest(ctx, "GET", fmt.Sprintf("/sessions/%d", c.sessionID), nil)
		if out.httpStatus == http.StatusNotFound {
			return c.newSession(ctx, SessionKindSql, true)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) QueryContext(ctx context.Context, query string) (GetStatementResponse, error) {
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

func (c *Client) ExecContext(ctx context.Context, query string) error {
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
	}

	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	resp, err := c.doRequest(ctx, "POST", "/sessions", body)
	if err != nil {
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

func (c *Client) waitForSessionToBeReady(ctx context.Context) error {
	var count int
	for {
		out, err := c.doRequest(ctx, "GET", fmt.Sprintf("/sessions/%d", c.sessionID), nil)
		if err != nil {
			return err
		}

		var resp GetSessionResponse
		if err := json.Unmarshal(out.body, &resp); err != nil {
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

func NewClient(ctx context.Context, url string, config map[string]any, jars []string, heartbeatTimeoutInSecond int) (Client, error) {
	client := Client{
		url:                             url,
		httpClient:                      &http.Client{},
		sessionConf:                     config,
		sessionJars:                     jars,
		sessionHeartbeatTimeoutInSecond: cmp.Or(heartbeatTimeoutInSecond, defaultHeartbeatTimeoutInSecond),
	}

	return client, nil
}
