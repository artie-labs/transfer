package main

import (
	"cmp"
	"context"
	"log"
	"log/slog"
	"os"

	"github.com/artie-labs/transfer/clients/postgres"
	"github.com/artie-labs/transfer/integration_tests/postgres/checks"
	"github.com/artie-labs/transfer/lib/config"
)

func main() {
	cfg := config.Postgres{
		Host:     cmp.Or(os.Getenv("PG_HOST"), "localhost"),
		Port:     5432,
		Username: "postgres",
		Password: "postgres",
		Database: "postgres",
	}

	ctx := context.Background()
	store, err := postgres.LoadStore(ctx, config.Config{Postgres: &cfg})
	if err != nil {
		log.Fatalf("failed to create postgres client: %v", err)
	}

	if err := checks.TestDialect(ctx, store, store.Dialect()); err != nil {
		log.Fatalf("failed to test dialect: %v", err)
	}

	if err := checks.TestStagingTable(ctx, store); err != nil {
		log.Fatalf("failed to test staging table: %v", err)
	}

	if err := checks.TestMergeOperations(ctx, store); err != nil {
		log.Fatalf("failed to test merge operations: %v", err)
	}

	slog.Info("Postgres integration tests all passed! 🎉")
}
