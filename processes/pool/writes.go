package pool

import (
	"context"
	"sync"
	"time"

	"github.com/artie-labs/transfer/processes/consumer"

	"github.com/artie-labs/transfer/lib/logger"
)

func StartPool(ctx context.Context, td time.Duration) {
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
			log.WithError(consumer.Flush(consumer.Args{
				Context: ctx,
			})).Info("Flushing via pool...")
		}
	}()

	// TODO - we're not doing anything with the wait?
}
