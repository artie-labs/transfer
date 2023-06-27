package snowflake

import (
	"context"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/dwh/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/snowflakedb/gosnowflake"
)

type Store struct {
	db.Store
	testDB    bool // Used for testing
	configMap *types.DwhToTablesConfigMap
}

const (
	// Column names from the output of DESC table;
	describeNameCol    = "name"
	describeTypeCol    = "type"
	describeCommentCol = "comment"
)

func (s *Store) Label() constants.DestinationKind {
	return constants.Snowflake
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) error {
	err := s.mergeWithStages(ctx, tableData)
	if AuthenticationExpirationErr(err) {
		logger.FromContext(ctx).WithError(err).Warn("authentication has expired, will reload the Snowflake store")
		s.ReestablishConnection(ctx)
	}

	return err
}

func (s *Store) ReestablishConnection(ctx context.Context) {
	if s.testDB {
		// Don't actually re-establish for tests.
		return
	}

	settings := config.FromContext(ctx)

	cfg := &gosnowflake.Config{
		Account:   settings.Config.Snowflake.AccountID,
		User:      settings.Config.Snowflake.Username,
		Password:  settings.Config.Snowflake.Password,
		Warehouse: settings.Config.Snowflake.Warehouse,
		Region:    settings.Config.Snowflake.Region,
	}

	if settings.Config.Snowflake.Host != "" {
		// If the host is specified
		cfg.Host = settings.Config.Snowflake.Host
		cfg.Region = ""
	}

	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		logger.FromContext(ctx).Fatalf("failed to get snowflake dsn, err: %v", err)
	}

	s.Store = db.Open(ctx, "snowflake", dsn)
	return
}

func LoadSnowflake(ctx context.Context, _store *db.Store, stages bool) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			testDB:    true,
			Store:     *_store,
			configMap: &types.DwhToTablesConfigMap{},
		}
	}

	s := &Store{
		configMap: &types.DwhToTablesConfigMap{},
	}

	s.ReestablishConnection(ctx)
	return s
}
