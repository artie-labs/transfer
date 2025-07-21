package postgres

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
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
