package snowflake

import (
	"fmt"
	"log/slog"

	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
)

const maxRetries = 10

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

func (s *Store) ToFullyQualifiedName(tableData *optimization.TableData, escape bool) string {
	return tableData.ToFqName(s.Label(), escape, s.config.SharedDestinationConfig.UppercaseEscapedNames, "")
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	fqName := s.ToFullyQualifiedName(tableData, true)
	return shared.GetTableConfig(shared.GetTableCfgArgs{
		Dwh:                s,
		FqName:             fqName,
		ConfigMap:          s.configMap,
		Query:              fmt.Sprintf("DESC table %s;", fqName),
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
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

func (s *Store) Merge(tableData *optimization.TableData) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = shared.Merge(s, tableData, s.config, types.MergeOpts{})
		if IsAuthExpiredError(err) {
			slog.Warn("Authentication has expired, will reload the Snowflake store and retry merging", slog.Any("err", err))
			s.reestablishConnection()
		} else {
			break
		}
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
