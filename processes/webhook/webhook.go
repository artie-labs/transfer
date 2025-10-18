package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"syscall"
	"time"

	"github.com/artie-labs/transfer/lib/retry"
)

const (
	// Retry configuration
	retryBaseMs    = 100
	retryMaxMs     = 5000
	maxRetries     = 3
	requestTimeout = 30 * time.Second
)

type Status string

const (
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
)

type Event string

const (
	EventMergeStarted  Event = "merge_started"
	EventMergeFinished Event = "merge_finished"
)

// Action represents a webhook event to be delivered
type Action struct {
	Event    Event          `json:"event"`
	Metadata map[string]any `json:"metadata,omitempty"`
	Status   Status         `json:"status"`
}

// Config holds webhook service configuration
type Config struct {
	// URLs is a list of webhook endpoints to deliver events to
	URLs []string
	// HTTPClient allows injection of custom HTTP client for testing
	HTTPClient *http.Client
	// BufferSize sets the channel buffer size (default: 1000)
	BufferSize int
}

// Service manages webhook delivery
type Service struct {
	config  Config
	actions chan Action
	client  *http.Client
}

// NewService creates a new webhook service
func NewService(cfg Config) *Service {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{
			Timeout: requestTimeout,
		}
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = 1000
	}

	return &Service{
		config:  cfg,
		actions: make(chan Action, cfg.BufferSize),
		client:  cfg.HTTPClient,
	}
}

// Publish sends an event to the webhook service for delivery
// This is non-blocking and returns immediately
func (s *Service) Publish(event Event, status Status, metadata map[string]any) {
	action := Action{
		Event:    event,
		Status:   status,
		Metadata: metadata,
	}

	select {
	case s.actions <- action:
		// Successfully queued
	default:
		slog.Warn("Webhook action channel is full, dropping event",
			slog.String("event", string(event)),
			slog.String("status", string(status)),
		)
	}
}

// Start begins processing webhook events in the background
func (s *Service) Start(ctx context.Context) {
	if len(s.config.URLs) == 0 {
		slog.Info("No webhook URLs configured, webhook service disabled")
		return
	}

	slog.Info("Starting webhook service",
		slog.Int("urls", len(s.config.URLs)),
		slog.Int("bufferSize", s.config.BufferSize),
	)

	go func() {
		for {
			select {
			case <-ctx.Done():
				slog.Info("Webhook service shutting down")
				return
			case action := <-s.actions:
				s.deliverToAllURLs(ctx, action)
			}
		}
	}()
}

// deliverToAllURLs delivers the action to all configured webhook URLs
func (s *Service) deliverToAllURLs(ctx context.Context, action Action) {
	for _, url := range s.config.URLs {
		if err := s.deliverWithRetry(ctx, url, action); err != nil {
			slog.Error("Failed to deliver webhook after retries",
				slog.String("url", url),
				slog.String("event", string(action.Event)),
				slog.Any("err", err),
			)
		}
	}
}

// deliverWithRetry attempts to deliver a webhook with exponential backoff
func (s *Service) deliverWithRetry(ctx context.Context, url string, action Action) error {
	retryCfg, err := retry.NewJitterRetryConfig(retryBaseMs, retryMaxMs, maxRetries, isRetryableHTTPError)
	if err != nil {
		return fmt.Errorf("failed to create retry config: %w", err)
	}

	return retry.WithRetries(retryCfg, func(attempt int, _ error) error {
		return s.deliver(ctx, url, action, attempt)
	})
}

// deliver sends a single webhook request
func (s *Service) deliver(ctx context.Context, url string, action Action, attempt int) error {
	payload, err := json.Marshal(action)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Artie-Transfer-Webhook/1.0")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read and discard body to reuse connection
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if attempt > 0 {
			slog.Info("Webhook delivered successfully after retry",
				slog.String("url", url),
				slog.String("event", string(action.Event)),
				slog.Int("attempt", attempt+1),
				slog.Int("statusCode", resp.StatusCode),
			)
		}
		return nil
	}

	return &httpError{
		statusCode: resp.StatusCode,
		url:        url,
	}
}

// httpError represents an HTTP error response
type httpError struct {
	statusCode int
	url        string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("webhook request to %s failed with status %d", e.url, e.statusCode)
}

// isRetryableHTTPError determines if an error should be retried
func isRetryableHTTPError(err error) bool {
	if err == nil {
		return false
	}

	// Retry network errors
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, io.EOF) {
		return true
	}

	// Retry HTTP 5xx errors and 429 (rate limit)
	var httpErr *httpError
	if errors.As(err, &httpErr) {
		return httpErr.statusCode >= 500 || httpErr.statusCode == 429
	}

	return false
}
