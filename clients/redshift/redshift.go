package redshift

import (
	"context"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
)

type Store struct {
	credentialsClause string
	bucket            string
	optionalS3Prefix  string
	configMap         *types.DwhToTablesConfigMap
	skipLgCols        bool
	config            config.Config

	db.Store
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Label() constants.DestinationKind {
	return constants.Redshift
}

func (s *Store) Merge(_ context.Context, tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, s.config, types.MergeOpts{
		UseMergeParts: true,
		// We are adding SELECT DISTINCT here for the temporary table as an extra guardrail.
		// Redshift does not enforce any row uniqueness and there could be potential LOAD errors which will cause duplicate rows to arise.
		SubQueryDedupe: true,
	})
}

const (
	describeNameCol        = "column_name"
	describeTypeCol        = "data_type"
	describeDescriptionCol = "description"
)

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	describeQuery, err := describeTableQuery(describeArgs{
		RawTableName: tableData.RawName(),
		Schema:       tableData.TopicConfig.Schema,
	})

	if err != nil {
		return nil, err
	}

	return shared.GetTableConfig(shared.GetTableCfgArgs{
		Dwh:                s,
		FqName:             tableData.ToFqName(s.Label(), true, s.config.SharedDestinationConfig.UppercaseEscapedNames, ""),
		ConfigMap:          s.configMap,
		Query:              describeQuery,
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeDescriptionCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: tableData.TopicConfig.DropDeletedColumns,
	})
}

func LoadRedshift(cfg config.Config, _store *db.Store) *Store {
	if _store != nil {
		// Used for tests.
		return &Store{
			configMap:  &types.DwhToTablesConfigMap{},
			skipLgCols: cfg.Redshift.SkipLgCols,
			config:     cfg,

			Store: *_store,
		}
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		cfg.Redshift.Host, cfg.Redshift.Port, cfg.Redshift.Username,
		cfg.Redshift.Password, cfg.Redshift.Database)

	return &Store{
		credentialsClause: cfg.Redshift.CredentialsClause,
		bucket:            cfg.Redshift.Bucket,
		optionalS3Prefix:  cfg.Redshift.OptionalS3Prefix,
		skipLgCols:        cfg.Redshift.SkipLgCols,
		configMap:         &types.DwhToTablesConfigMap{},
		config:            cfg,

		Store: db.Open("postgres", connStr),
	}
}
