package apachelivy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Client struct {
	url         string
	sessionID   int
	httpClient  *http.Client
	sessionConf map[string]any
	sessionJars []string
}

func (c Client) QueryContext(ctx context.Context, query string) (GetStatementResponse, error) {
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

func (c Client) ExecContext(ctx context.Context, query string) error {
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

func (c Client) waitForStatement(ctx context.Context, statementID int) (GetStatementResponse, error) {
	for {
		respBytes, err := c.doRequest(ctx, "GET", fmt.Sprintf("/sessions/%d/statements/%d", c.sessionID, statementID), nil)
		if err != nil {
			return GetStatementResponse{}, err
		}

		var resp GetStatementResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
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
		time.Sleep(1 * time.Second)
	}
}

func (c Client) submitLivyStatement(ctx context.Context, code string) (int, error) {
	reqBody, err := json.Marshal(CreateStatementRequest{Code: code, Kind: "sql"})
	if err != nil {
		return 0, err
	}

	respBytes, err := c.doRequest(ctx, "POST", fmt.Sprintf("/sessions/%d/statements", c.sessionID), reqBody)
	if err != nil {
		return 0, err
	}

	var resp CreateStatementResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return 0, err
	}

	return resp.ID, nil
}

func (c Client) doRequest(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.url+path, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return out, nil
}

func (c *Client) newSession(ctx context.Context, kind string, blockUntilReady bool) error {
	body, err := json.Marshal(CreateSessionRequest{
		Kind: kind,
		Jars: c.sessionJars,
		Conf: c.sessionConf,
	})
	if err != nil {
		return err
	}

	resp, err := c.doRequest(ctx, "POST", "/sessions", body)
	if err != nil {
		slog.Warn("Failed to create session", slog.Any("err", err), slog.String("response", string(resp)))
		return err
	}

	var createResp CreateSessionResponse
	if err = json.Unmarshal(resp, &createResp); err != nil {
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

func (c Client) waitForSessionToBeReady(ctx context.Context) error {
	for {
		respBytes, err := c.doRequest(ctx, "GET", fmt.Sprintf("/sessions/%d", c.sessionID), nil)
		if err != nil {
			return err
		}

		var resp GetSessionResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return err
		}

		switch resp.State {
		case StateIdle:
			return nil
		case StateNotStarted, StateStarting:
			slog.Info("Session not ready", slog.Any("resp", resp))
			slog.Info("Sleeping for 1 second")
			time.Sleep(1 * time.Second)
		default:
			return fmt.Errorf("session in unexpected state: %q", resp.State)
		}
	}
}

func NewClient(ctx context.Context, url string, config map[string]any) (Client, error) {
	client := Client{
		url:         url,
		httpClient:  &http.Client{},
		sessionConf: config,
	}

	// https://livy.incubator.apache.org/docs/latest/rest-api.html#session-kind
	if err := client.newSession(ctx, "sql", true); err != nil {
		return Client{}, err
	}

	slog.Info("Session has been created in Apache Livy", slog.Any("sessionID", client.sessionID))
	return client, nil
}
