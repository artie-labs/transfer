package lib

import (
	"log/slog"
	"time"
)

type Heartbeats struct {
	startTime time.Time
	// [initialDelay] - The time to wait before starting the heartbeat.
	initialDelay time.Duration
	// [intervalTicker] - The interval at which to send heartbeats ping.
	intervalTicker time.Duration

	// Metadata
	metric string
	tags   map[string]any
}

func NewHeartbeats(initialDelay time.Duration, intervalTicker time.Duration, metric string, tags map[string]any) *Heartbeats {
	return &Heartbeats{
		initialDelay:   initialDelay,
		intervalTicker: intervalTicker,
		metric:         metric,
		tags:           tags,
	}
}

func (h *Heartbeats) Start() func() {
	h.startTime = time.Now()

	// Create a channel to signal the heartbeat goroutine to stop
	done := make(chan struct{})

	// Start the heartbeat goroutine
	go h.start(done)

	// Return a function to stop the heartbeat goroutine
	return func() {
		close(done)
	}
}

// Start begins monitoring for deadlocks. It waits for startTime, then logs a heartbeat every interval until done is closed.
func (h *Heartbeats) start(done <-chan struct{}) {
	// Wait for the initial delay before starting heartbeats
	timer := time.NewTimer(h.initialDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		// Start the ticker after the initial delay
	case <-done:
		return
	}

	ticker := time.NewTicker(h.intervalTicker)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			slog.Info("Heartbeat check", slog.String("metric", h.metric), slog.Any("tags", h.tags), slog.Duration("duration", time.Since(h.startTime)))
		}
	}
}
