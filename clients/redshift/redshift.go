package redshift

import (
	"context"
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

func LoadRedshift(ctx context.Context, _store *db.Store) *Store {
	settings := config.FromContext(ctx)

	if _store != nil {
		// Used for tests.
		return &Store{
			configMap:         &types.DwhToTablesConfigMap{},
			skipLgCols:        settings.Config.Redshift.SkipLgCols,
			uppercaseEscNames: settings.Config.SharedDestinationConfig.UppercaseEscapedNames,

			Store: *_store,
		}
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		settings.Config.Redshift.Host, settings.Config.Redshift.Port, settings.Config.Redshift.Username,
		settings.Config.Redshift.Password, settings.Config.Redshift.Database)

	return &Store{
		credentialsClause: settings.Config.Redshift.CredentialsClause,
		bucket:            settings.Config.Redshift.Bucket,
		optionalS3Prefix:  settings.Config.Redshift.OptionalS3Prefix,
		skipLgCols:        settings.Config.Redshift.SkipLgCols,
		configMap:         &types.DwhToTablesConfigMap{},
		uppercaseEscNames: settings.Config.SharedDestinationConfig.UppercaseEscapedNames,

		Store: db.Open("postgres", connStr),
	}
}
