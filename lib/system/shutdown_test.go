//go:build unix

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

func TestShutdownHook_runsCleanupAndCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cleaned atomic.Bool
	cleanup := func() { cleaned.Store(true) }

	logger := slog.New(slog.DiscardHandler)

	ShutdownHook(logger, cleanup, cancel)

	// Let the goroutine register with signal.Notify before we signal.
	time.Sleep(50 * time.Millisecond)

	if err := syscall.Kill(os.Getpid(), syscall.SIGINT); err != nil {
		t.Fatalf("send SIGINT: %v", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("expected context cancelled after signal")
	}

	if !cleaned.Load() {
		t.Fatal("expected cleanup to run")
	}
}
