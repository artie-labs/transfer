package main

import (
	"context"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/models"
	"os"
	"sync"
	"time"

	"github.com/artie-labs/transfer/clients/snowflake"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/processes/kafka"
	"github.com/artie-labs/transfer/processes/pool"
)

func main() {
	// Parse args into settings.
	config.ParseArgs(os.Args)

	ctx := logger.InjectLoggerIntoCtx(logger.NewLogger(config.GetSettings()), context.Background())
	snowflake.InitSnowflake(ctx, nil)
	models.InitMemoryDB()

	flushChan := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		pool.StartPool(ctx, 10*time.Second, flushChan)
	}(ctx)

	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		kafka.StartConsumer(ctx, flushChan)
	}(ctx)

	wg.Wait()
}
