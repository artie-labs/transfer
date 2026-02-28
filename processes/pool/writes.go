package pool

import (
	"context"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/typing"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
)

func StartPool(ctx context.Context, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client, whClient *webhooksclient.Client, topics []string, td time.Duration) {
	slog.Info("Starting pool timer...")
	ticker := time.NewTicker(td)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("Pool timer stopped due to context cancellation")
			return
		case <-ticker.C:
			slog.Info("Flushing via pool...")
			if err := consumer.Flush(ctx, inMemDB, dest, metricsClient, whClient, topics, consumer.Args{Reason: "time", CoolDown: typing.ToPtr(td)}); err != nil {
				slog.Error("Failed to flush via pool", slog.Any("err", err))
			}
		}
	}
}
