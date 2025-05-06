package apachelivy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
)

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
