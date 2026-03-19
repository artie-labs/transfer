package pool

import (
	"context"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/webhooks"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
)

func StartPool(ctx context.Context, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client, whClient *webhooks.Client, topics []string, td time.Duration, cfg config.Config) {
	slog.Info("Starting pool timer...")
	ticker := time.NewTicker(td)
	for range ticker.C {
		slog.Info("Flushing via pool...")
		if err := consumer.Flush(ctx, inMemDB, dest, metricsClient, whClient, topics, consumer.Args{Reason: "time", CoolDown: typing.ToPtr(td), ReportDBExecutionTime: cfg.Reporting.EmitDBExecutionTime}); err != nil {
			slog.Error("Failed to flush via pool", slog.Any("err", err))
		}
	}
}
