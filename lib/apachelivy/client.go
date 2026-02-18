package apachelivy

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/retry"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

const (
	sleepBaseMs                     = 1_000
	sleepMaxMs                      = 3_000
	defaultHeartbeatTimeoutInSecond = 300
	maxSessionRetries               = 500
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
	sessionName                     string
	metricsClient                   base.Client

	lastChecked time.Time
}

func (c *Client) buildRetryConfig() (retry.RetryConfig, error) {
	cfg, err := retry.NewJitterRetryConfig(sleepBaseMs, sleepMaxMs, maxSessionRetries, shouldRetry)
	if err != nil {
		return nil, fmt.Errorf("failed to create retry config: %w", err)
	}

	return cfg, nil
}

func (c *Client) queryContext(ctx context.Context, query string, attempt int) (GetStatementResponse, error) {
	if err := c.ensureSession(ctx, attempt > 0); err != nil {
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

	return retry.WithRetriesAndResult(retryCfg, func(attempt int, _ error) (GetStatementResponse, error) {
		return c.queryContext(ctx, query, attempt)
	})
}

func (c *Client) execContext(ctx context.Context, query string, attempt int) error {
	if err := c.ensureSession(ctx, attempt > 0); err != nil {
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

	return retry.WithRetries(retryCfg, func(attempt int, _ error) error {
		return c.execContext(ctx, query, attempt)
	})
}

func (c *Client) waitForStatement(ctx context.Context, statementID int) (GetStatementResponse, error) {
	var count int
	var lastState string
	var executingStartTime time.Time
	startTime := time.Now()

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
			if err := resp.Error(c.sessionID); err != nil {
				return GetStatementResponse{}, err
			}

			if c.metricsClient != nil {
				tags := map[string]string{"sessionName": c.sessionName}
				if !executingStartTime.IsZero() {
					c.metricsClient.Gauge("livy.statement.execution_ms", float64(time.Since(executingStartTime).Milliseconds()), tags)
				}
				c.metricsClient.Gauge("livy.statement.total_ms", float64(time.Since(startTime).Milliseconds()), tags)
			}

			return resp, nil
		}

		if resp.State != lastState {
			lastState = resp.State
			if resp.State == "running" && executingStartTime.IsZero() {
				if c.metricsClient != nil {
					c.metricsClient.Gauge("livy.statement.queued_ms", float64(time.Since(startTime).Milliseconds()), map[string]string{"sessionName": c.sessionName})
				}
				executingStartTime = time.Now()
			}
		}

		sleepTime := jitter.Jitter(sleepBaseMs, sleepMaxMs, count)
		switch resp.State {
		case "waiting":
			slog.Info("Statement is queued, waiting for execution", slog.Int("sessionID", c.sessionID), slog.Int("statementID", statementID), slog.Duration("sleepTime", sleepTime))
		case "running":
			slog.Info("Statement is executing", slog.Int("sessionID", c.sessionID), slog.Int("statementID", statementID), slog.Duration("sleepTime", sleepTime))
		default:
			slog.Info("Statement is not ready yet", slog.Int("sessionID", c.sessionID), slog.Int("statementID", statementID), slog.String("state", resp.State), slog.Duration("sleepTime", sleepTime))
		}

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

func NewClient(url string, config map[string]any, jars []string, heartbeatTimeoutInSecond int, driverMemory, executorMemory, sessionName string) *Client {
	return &Client{
		url:                             url,
		httpClient:                      &http.Client{},
		sessionConf:                     config,
		sessionJars:                     jars,
		sessionHeartbeatTimeoutInSecond: cmp.Or(heartbeatTimeoutInSecond, defaultHeartbeatTimeoutInSecond),
		sessionDriverMemory:             driverMemory,
		sessionExecutorMemory:           executorMemory,
		sessionName:                     sessionName,
	}
}

func (c *Client) SetMetricsClient(metricsClient base.Client) {
	c.metricsClient = metricsClient
}

func (c *Client) WithPriorityClient() *Client {
	if strings.HasSuffix(c.sessionName, "-priority") {
		return c
	}

	// Check if the current config has [SparkDriverSelector]
	selectorValue, ok := c.sessionConf[SparkDriverSelector]
	if !ok {
		// Return the same client since it doesn't have the priority selector
		return c
	}

	// Now check if [SparkExecutorSelector] is also set
	if val := c.sessionConf[SparkExecutorSelector]; selectorValue == val {
		// If both selectors are set to the same value, this is a priority client, so just return the same client.
		return c
	}

	// Clone, so we don't mutate the original configuration.
	sessionConfig := maps.Clone(c.sessionConf)
	sessionConfig[SparkExecutorSelector] = selectorValue

	// If [SparkExecutorSelector] is not set, but [SparkDriverSelector] is set, then we need to create a new client with the priority selector.
	priorityClient := NewClient(c.url, sessionConfig, c.sessionJars, c.sessionHeartbeatTimeoutInSecond, c.sessionDriverMemory, c.sessionExecutorMemory, fmt.Sprintf("%s-priority", c.sessionName))
	priorityClient.SetMetricsClient(c.metricsClient)
	return priorityClient
}
