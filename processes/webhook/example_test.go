package webhook_test

import (
	"context"

	"github.com/artie-labs/transfer/processes/webhook"
)

// Example shows how to use the webhook service in your application
func Example() {
	// 1. Create a webhook service with configuration
	svc := webhook.NewService(webhook.Config{
		URLs: []string{
			"https://your-app.com/webhooks/artie",
			"https://backup-receiver.com/notify",
		},
		BufferSize: 1000, // Optional: defaults to 1000
	})

	// 2. Start the service with your application context
	ctx := context.Background()
	svc.Start(ctx)

	// 3. Publish events from anywhere in your codebase
	// The service handles delivery, retries, and error handling automatically

	// Example: Publishing a merge start event
	svc.Publish(
		webhook.EventMergeStarted,
		webhook.StatusSuccess,
		map[string]any{
			"table":     "users",
			"database":  "production",
			"timestamp": "2024-01-15T10:30:00Z",
		},
	)

	// Example: Publishing a merge completion event
	svc.Publish(
		webhook.EventMergeFinished,
		webhook.StatusSuccess,
		map[string]any{
			"table":     "users",
			"database":  "production",
			"rows":      1500,
			"duration":  "2.5s",
			"timestamp": "2024-01-15T10:30:02Z",
		},
	)

	// Example: Publishing a failure event
	svc.Publish(
		webhook.EventMergeFinished,
		webhook.StatusFailed,
		map[string]any{
			"table":    "orders",
			"database": "production",
			"error":    "connection timeout",
		},
	)

	// The service will:
	// - Queue events in a buffered channel (non-blocking)
	// - Deliver to all configured URLs in parallel
	// - Automatically retry on network errors and 5xx responses
	// - Log errors after all retry attempts are exhausted
	// - Gracefully shutdown when context is cancelled
}
