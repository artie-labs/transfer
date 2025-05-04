package apachelivy

import (
	"context"
	"net/http"
	"slices"
	"time"
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

func (c *Client) ensureSession(ctx context.Context, forceCheck bool) error {
	if c.sessionID == 0 {
		c.lastChecked = time.Now()
		return c.newSession(ctx, SessionKindSql, true)
	}

	if forceCheck || time.Since(c.lastChecked).Seconds() > (float64(c.sessionHeartbeatTimeoutInSecond)-sessionBufferSeconds) {
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
