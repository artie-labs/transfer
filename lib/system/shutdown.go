package system

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func ShutdownHook(cleanUpHandlers func(), cancel context.CancelFunc) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		signal.Stop(sigCh) // allow a second signal to terminate if cleanup blocks
		slog.Info("Received shutdown signal, initiating graceful shutdown...", slog.String("signal", sig.String()))

		cleanUpHandlers()
		cancel()
	}()
}
