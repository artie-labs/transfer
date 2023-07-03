package pool

import (
	"context"
	"time"

	"github.com/artie-labs/transfer/processes/consumer"

	"github.com/artie-labs/transfer/lib/logger"
)

func StartPool(ctx context.Context, td time.Duration) {
	log := logger.FromContext(ctx)
	log.Info("Starting pool timer...")
	ticker := time.NewTicker(td)
	for range ticker.C {
		log.WithError(consumer.Flush(consumer.Args{
			Context: ctx,
		})).Info("Flushing via pool...")
	}
}
