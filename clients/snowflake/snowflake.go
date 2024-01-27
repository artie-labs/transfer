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
	uppercaseEscNames bool
	testDB            bool // Used for testing
	configMap         *types.DwhToTablesConfigMap
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
	err := s.mergeWithStages(ctx, tableData)
	if IsAuthExpiredError(err) {
		slog.Warn("authentication has expired, will reload the Snowflake store and retry merging", slog.Any("err", err))
		s.reestablishConnection(ctx)
		return s.Merge(ctx, tableData)
	}

	return err
}

func (s *Store) reestablishConnection(ctx context.Context) {
	if s.testDB {
		// Don't actually re-establish for tests.
		return
	}

	settings := config.FromContext(ctx)
	cfg := &gosnowflake.Config{
		Account:     settings.Config.Snowflake.AccountID,
		User:        settings.Config.Snowflake.Username,
		Password:    settings.Config.Snowflake.Password,
		Warehouse:   settings.Config.Snowflake.Warehouse,
		Region:      settings.Config.Snowflake.Region,
		Application: settings.Config.Snowflake.Application,
	}

	if settings.Config.Snowflake.Host != "" {
		// If the host is specified
		cfg.Host = settings.Config.Snowflake.Host
		cfg.Region = ""
	}

	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		logger.Panic("failed to get snowflake dsn", slog.Any("err", err))
	}

	s.Store = db.Open(ctx, "snowflake", dsn)
}

func LoadSnowflake(ctx context.Context, _store *db.Store) *Store {
	cfg := config.FromContext(ctx).Config

	if _store != nil {
		// Used for tests.
		return &Store{
			testDB:            true,
			uppercaseEscNames: cfg.SharedDestinationConfig.UppercaseEscapedNames,
			configMap:         &types.DwhToTablesConfigMap{},

			Store: *_store,
		}
	}

	s := &Store{
		uppercaseEscNames: cfg.SharedDestinationConfig.UppercaseEscapedNames,
		configMap:         &types.DwhToTablesConfigMap{},
	}

	s.reestablishConnection(ctx)
	return s
}
