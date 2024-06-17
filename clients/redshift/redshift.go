package redshift

import (
	"fmt"
	"strings"

	_ "github.com/lib/pq"

	"github.com/artie-labs/transfer/clients/redshift/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type Store struct {
	credentialsClause string
	bucket            string
	optionalS3Prefix  string
	configMap         *types.DwhToTablesConfigMap
	config            config.Config

	db.Store
}

func (s *Store) Append(tableData *optimization.TableData) error {
	return shared.Append(s, tableData, types.AdditionalSettings{})
}

func (s *Store) Merge(tableData *optimization.TableData) error {
	return shared.Merge(s, tableData, types.MergeOpts{
		// We are adding SELECT DISTINCT here for the temporary table as an extra guardrail.
		// Redshift does not enforce any row uniqueness and there could be potential LOAD errors which will cause duplicate rows to arise.
		SubQueryDedupe: true,
	})
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return NewTableIdentifier(topicConfig.Schema, table)
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Dialect() sql.Dialect {
	return dialect.RedshiftDialect{}
}

func (s *Store) AdditionalDateFormats() []string {
	return s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	const (
		describeNameCol        = "column_name"
		describeTypeCol        = "data_type"
		describeDescriptionCol = "description"
	)

	query, args := describeTableQuery(describeArgs{
		RawTableName: tableData.Name(),
		Schema:       tableData.TopicConfig().Schema,
	})

	return shared.GetTableCfgArgs{
		Dwh:                s,
		TableID:            s.IdentifierFor(tableData.TopicConfig(), tableData.Name()),
		ConfigMap:          s.configMap,
		Query:              query,
		Args:               args,
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeDescriptionCol,
		EmptyCommentValue:  ptr.ToString("<nil>"),
		DropDeletedColumns: tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) Sweep() error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	// `relkind` will filter for only ordinary tables and exclude sequences, views, etc.
	queryFunc := func(topicConfig kafkalib.TopicConfig) (string, []any) {
		return `
SELECT
    n.nspname, c.relname
FROM
    PG_CATALOG.PG_CLASS c
JOIN
    PG_CATALOG.PG_NAMESPACE n ON n.oid = c.relnamespace
WHERE
    n.nspname = $1 AND c.relname ILIKE $2 AND c.relkind = 'r';`, []any{topicConfig.Schema, "%" + constants.ArtiePrefix + "%"}
	}

	return shared.Sweep(s, tcs, queryFunc)
}

func (s *Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))
	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)
	return destination.ExecStatements(s, dedupeQueries)
}

func LoadRedshift(cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,

			Store: *_store,
		}, nil
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		cfg.Redshift.Host, cfg.Redshift.Port, cfg.Redshift.Username,
		cfg.Redshift.Password, cfg.Redshift.Database)

	store, err := db.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return &Store{
		credentialsClause: cfg.Redshift.CredentialsClause,
		bucket:            cfg.Redshift.Bucket,
		optionalS3Prefix:  cfg.Redshift.OptionalS3Prefix,
		configMap:         &types.DwhToTablesConfigMap{},
		config:            cfg,

		Store: store,
	}, nil
}
