package postgres

import (
	"context"
	"fmt"

	"github.com/artie-labs/transfer/clients/postgres/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	configMap *types.DestinationTableConfigMap
	config    config.Config

	db.Store
}

func LoadStore(cfg config.Config) (*Store, error) {
	store, err := db.Open("postgres", cfg.Postgres.DSN())
	if err != nil {
		return nil, err
	}

	return &Store{
		Store:     store,
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
	}, nil
}

func (s Store) Dialect() sql.Dialect {
	return dialect.PostgresDialect{}
}

func (s Store) GetConfig() config.Config {
	return s.config
}

func (s Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	if !tableID.AllowToDrop() {
		return fmt.Errorf("table %q is not allowed to be dropped", tableID.FullyQualifiedName())
	}

	if _, err := s.ExecContext(ctx, s.Dialect().BuildDropTableQuery(tableID)); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// We'll then clear it from our cache
	s.configMap.RemoveTable(tableID)
	return nil
}
