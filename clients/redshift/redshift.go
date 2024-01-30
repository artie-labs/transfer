package redshift

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/optimization"

	"github.com/artie-labs/transfer/clients/utils"

	"github.com/artie-labs/transfer/lib/ptr"

	"github.com/artie-labs/transfer/lib/config"
	_ "github.com/lib/pq"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination/types"
)

type Store struct {
	credentialsClause string
	bucket            string
	optionalS3Prefix  string
	configMap         *types.DwhToTablesConfigMap
	skipLgCols        bool
	uppercaseEscNames bool

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

const (
	describeNameCol        = "column_name"
	describeTypeCol        = "data_type"
	describeDescriptionCol = "description"
)

func (s *Store) getTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	describeQuery, err := describeTableQuery(describeArgs{
		RawTableName: tableData.RawName(),
		Schema:       tableData.TopicConfig.Schema,
	})

	if err != nil {
		return nil, err
	}

	return utils.GetTableConfig(utils.GetTableCfgArgs{
		Dwh:                s,
		FqName:             tableData.ToFqName(s.Label(), true, s.uppercaseEscNames, ""),
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
			configMap:         &types.DwhToTablesConfigMap{},
			skipLgCols:        cfg.Redshift.SkipLgCols,
			uppercaseEscNames: cfg.SharedDestinationConfig.UppercaseEscapedNames,

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
		uppercaseEscNames: cfg.SharedDestinationConfig.UppercaseEscapedNames,

		Store: db.Open("postgres", connStr),
	}
}
