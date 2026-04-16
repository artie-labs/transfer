package system

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func ShutdownHook(logger *slog.Logger, cleanUpHandlers func(), cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		slog.Info("Received shutdown signal, initiating graceful shutdown...", slog.String("signal", sig.String()))

		cleanUpHandlers()
		cancel()
	}()
}
