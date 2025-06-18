package lib

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	fake "github.com/artie-labs/transfer/lib/mocks"
)

func TestHeartbeats_LogsAfterInitialDelayAndInterval(t *testing.T) {
	var count int32
	fakeHandler := &fake.FakeHandler{}
	fakeHandler.HandleStub = func(_ context.Context, r slog.Record) error {
		if r.Level == slog.LevelInfo {
			atomic.AddInt32(&count, 1)
		}
		return nil
	}
	slog.SetDefault(slog.New(fakeHandler))

	hb := NewHeartbeats(50*time.Millisecond, 30*time.Millisecond, "test_metric", map[string]any{"foo": "bar"})
	cancel := hb.Start()
	defer cancel()

	time.Sleep(120 * time.Millisecond) // Should see at least 2 logs
	logged := atomic.LoadInt32(&count)
	if logged < 2 {
		t.Errorf("expected at least 2 heartbeats, got %d", logged)
	}
}

func TestHeartbeats_StopsOnCancel(t *testing.T) {
	var count int32
	fakeHandler := &fake.FakeHandler{}
	fakeHandler.HandleStub = func(_ context.Context, r slog.Record) error {
		if r.Level == slog.LevelInfo {
			atomic.AddInt32(&count, 1)
		}
		return nil
	}
	slog.SetDefault(slog.New(fakeHandler))

	hb := NewHeartbeats(10*time.Millisecond, 20*time.Millisecond, "test_metric", nil)
	cancel := hb.Start()
	time.Sleep(40 * time.Millisecond)
	cancel()
	loggedBefore := atomic.LoadInt32(&count)
	time.Sleep(50 * time.Millisecond)
	loggedAfter := atomic.LoadInt32(&count)
	if loggedAfter != loggedBefore {
		t.Errorf("expected no more heartbeats after cancel, got %d more", loggedAfter-loggedBefore)
	}
}

func TestHeartbeats_DoesNotLogBeforeInitialDelay(t *testing.T) {
	var count int32
	fakeHandler := &fake.FakeHandler{}
	fakeHandler.HandleStub = func(_ context.Context, r slog.Record) error {
		if r.Level == slog.LevelInfo {
			atomic.AddInt32(&count, 1)
		}
		return nil
	}
	slog.SetDefault(slog.New(fakeHandler))

	hb := NewHeartbeats(100*time.Millisecond, 50*time.Millisecond, "test_metric", nil)
	cancel := hb.Start()
	defer cancel()

	time.Sleep(60 * time.Millisecond)
	if got := atomic.LoadInt32(&count); got != 0 {
		t.Errorf("expected 0 heartbeats before initial delay, got %d", got)
	}
}

func TestHeartbeats_StartStopMultipleTimes(t *testing.T) {
	for i := 0; i < 3; i++ {
		fakeHandler := &fake.FakeHandler{}
		slog.SetDefault(slog.New(fakeHandler))
		hb := NewHeartbeats(5*time.Millisecond, 5*time.Millisecond, "test_metric", nil)
		cancel := hb.Start()
		time.Sleep(15 * time.Millisecond)
		cancel()
	}
}
