package pool

import (
	"context"
	"sync"
	"time"

	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/models/flush"
)

func StartPool(ctx context.Context, td time.Duration, flushChannel chan bool) {
	// To start pool, we will have 2 go routines running.
	// Go Routine #1 - running with the Ticker.
	// Go Routine #2 - running with a blocking channel such that we can act on signals (like pool size has exceeded flush threshold)

	var wg sync.WaitGroup
	log := logger.FromContext(ctx)
	wg.Add(1)
	go func() {
		log.Info("Starting pool timer...")
		defer wg.Done()
		ticker := time.NewTicker(td)
		for range ticker.C {
			log.WithError(flush.Flush(ctx)).Info("Flushing via pool...")
		}
	}()

	wg.Add(1)
	go func(fChannel chan bool) {
		log.Info("Starting pool channel...")
		defer wg.Done()

		for range fChannel {
			log.WithError(flush.Flush(ctx)).Info("Flushing via channel...")
		}
	}(flushChannel)

}
