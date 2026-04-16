package system

import (
	"context"
	"log/slog"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestShutdownOnSignal_runsCleanupAndCancelsContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cleaned atomic.Bool
	cleanup := func() { cleaned.Store(true) }

	sigCh := make(chan os.Signal, 1)
	logger := slog.New(slog.DiscardHandler)

	shutdownOnSignal(sigCh, logger, cleanup, cancel)

	sigCh <- syscall.SIGINT

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("expected context cancelled after signal")
	}

	if !cleaned.Load() {
		t.Fatal("expected cleanup to run")
	}
}
