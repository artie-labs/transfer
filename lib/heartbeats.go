package lib

import (
	"log/slog"
	"time"
)

// Heartbeats is a struct that allows us to monitor long running processes to spot potential deadlocks
// To use heartbeats, you'll first want to create the struct by invoking [NewHeartbeats]
// Once that's done, you'll then want to start the function via [Start] which returns a done function that should be used invoked by a defer.
type Heartbeats struct {
	startTime time.Time
	// [initialDelay] - The time to wait before starting the heartbeat. If the main process finishes before the initial delay, heartbeats will not be started.
	// This is done to prevent noise in our logs.
	initialDelay time.Duration
	// [intervalTicker] - The interval at which to send heartbeats ping.
	intervalTicker time.Duration

	// Used for logging
	metric string
	tags   map[string]any

	// [test] - If true, the heartbeat will be ticked.
	test  bool
	ticks int
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

func (h *Heartbeats) start(done <-chan struct{}) {
	// Wait for the initial delay before starting heartbeats
	timer := time.NewTimer(h.initialDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-done:
		return
	}

	// We're here if the initial delay has been passed
	ticker := time.NewTicker(h.intervalTicker)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if h.test {
				h.ticks++
			}

			slog.Info("[Heartbeats] Process is still running",
				slog.String("metric", h.metric),
				slog.Any("tags", h.tags),
				slog.Duration("duration", time.Since(h.startTime)),
			)
		}
	}
}
