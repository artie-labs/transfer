package webhook

import (
	"context"

	"github.com/artie-labs/transfer/lib/config"
)

// LoadFromConfig creates and starts a webhook service from the configuration
// Returns nil if webhooks are not configured
func LoadFromConfig(ctx context.Context, cfg config.Config) *Service {
	if cfg.Webhooks == nil || len(cfg.Webhooks.URLs) == 0 {
		return nil
	}

	svc := NewService(Config{
		URLs:       cfg.Webhooks.URLs,
		BufferSize: cfg.Webhooks.BufferSize,
	})

	svc.Start(ctx)
	return svc
}
