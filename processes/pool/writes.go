package pool

import (
	"context"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/processes/consumer"
)

func StartPool(ctx context.Context, td time.Duration) {
	slog.Info("Starting pool timer...")
	ticker := time.NewTicker(td)
	for range ticker.C {
		slog.With("err", consumer.Flush(consumer.Args{
			Reason:   "time",
			Context:  ctx,
			CoolDown: ptr.ToDuration(td),
		})).Info("Flushing via pool...")
	}
}
