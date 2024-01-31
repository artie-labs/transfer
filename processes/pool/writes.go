package pool

import (
	"context"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
)

func StartPool(ctx context.Context, inMemDB *models.DatabaseData, dest destination.Baseline, td time.Duration) {
	slog.Info("Starting pool timer...")
	ticker := time.NewTicker(td)
	for range ticker.C {
		slog.Info("Flushing via pool...")
		if err := consumer.Flush(ctx, inMemDB, dest, consumer.Args{
			Reason:   "time",
			CoolDown: ptr.ToDuration(td),
		}); err != nil {
			slog.Error("Failed to flush via pool", slog.Any("err", err))
		}
	}
}
