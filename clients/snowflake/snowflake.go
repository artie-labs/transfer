package snowflake

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/artie-labs/transfer/clients/utils"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/snowflakedb/gosnowflake"
)

type Store struct {
	db.Store
	testDB    bool // Used for testing
	configMap *types.DwhToTablesConfigMap
	config    config.Config
}

const (
	// Column names from the output of DESC table;
	describeNameCol    = "name"
	describeTypeCol    = "type"
	describeCommentCol = "comment"
)

func (s *Store) getTableConfig(fqName string, dropDeletedColumns bool) (*types.DwhTableConfig, error) {
	return utils.GetTableConfig(utils.GetTableCfgArgs{
		Dwh:                s,
		FqName:             fqName,
		ConfigMap:          s.configMap,
		Query:              fmt.Sprintf("DESC table %s;", fqName),
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: dropDeletedColumns,
	})
}

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
	err := s.mergeWithStages(tableData)
	if IsAuthExpiredError(err) {
		slog.Warn("authentication has expired, will reload the Snowflake store and retry merging", slog.Any("err", err))
		s.reestablishConnection()
		return s.Merge(ctx, tableData)
	}

	return err
}

func (s *Store) reestablishConnection() {
	if s.testDB {
		// Don't actually re-establish for tests.
		return
	}

	cfg := &gosnowflake.Config{
		Account:     s.config.Snowflake.AccountID,
		User:        s.config.Snowflake.Username,
		Password:    s.config.Snowflake.Password,
		Warehouse:   s.config.Snowflake.Warehouse,
		Region:      s.config.Snowflake.Region,
		Application: s.config.Snowflake.Application,
	}

	if s.config.Snowflake.Host != "" {
		// If the host is specified
		cfg.Host = s.config.Snowflake.Host
		cfg.Region = ""
	}

	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		logger.Panic("Failed to get snowflake dsn", slog.Any("err", err))
	}

	s.Store = db.Open("snowflake", dsn)
}

func LoadSnowflake(cfg config.Config, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			testDB:    true,
			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,

			Store: *_store,
		}
	}

	s := &Store{
		configMap: &types.DwhToTablesConfigMap{},
		config:    cfg,
	}

	s.reestablishConnection()
	return s
}
