package pool

import (
	"context"
	"log/slog"
	"time"

	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/processes/consumer"
)

func StartPool[M any](ctx context.Context, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client, topics []string, td time.Duration) {
	slog.Info("Starting pool timer...")
	ticker := time.NewTicker(td)
	for range ticker.C {
		slog.Info("Flushing via pool...")
		if err := consumer.Flush[M](ctx, inMemDB, dest, metricsClient, topics, consumer.Args{Reason: "time", CoolDown: typing.ToPtr(td)}); err != nil {
			slog.Error("Failed to flush via pool", slog.Any("err", err))
		}
	}
}
