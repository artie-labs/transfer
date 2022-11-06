package pool

import (
	"context"
	"time"

	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/models/flush"
)

func StartPool(ctx context.Context, td time.Duration) {
	ticker := time.NewTicker(td)
	for range ticker.C {
		logger.FromContext(ctx).WithError(flush.Flush(ctx)).Info("Flushing...")
	}
}
