package redshift

import (
	"context"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/artie-labs/transfer/clients/redshift/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	credentialsClause string
	bucket            string
	optionalS3Prefix  string
	configMap         *types.DestinationTableConfigMap
	config            config.Config

	db.Store
}

func (s *Store) DropTable(_ context.Context, _ sql.TableIdentifier) error {
	return fmt.Errorf("not supported")
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, _ bool) error {
	return shared.Append(ctx, s, tableData, types.AdditionalSettings{})
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
	if err := shared.Merge(ctx, s, tableData, types.MergeOpts{
		// We are adding SELECT DISTINCT here for the temporary table as an extra guardrail.
		// Redshift does not enforce any row uniqueness and there could be potential LOAD errors which will cause duplicate rows to arise.
		SubQueryDedupe: true,
	}); err != nil {
		return false, fmt.Errorf("failed to merge: %w", err)
	}

	return true, nil
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(topicConfig.Schema, table)
}

func (s *Store) GetConfigMap() *types.DestinationTableConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s *Store) dialect() dialect.RedshiftDialect {
	return dialect.RedshiftDialect{}
}

func (s *Store) GetTableConfig(tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Dwh:                   s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "column_name",
		ColumnNameForDataType: "data_type",
		ColumnNameForComment:  "description",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) SweepTemporaryTables(_ context.Context) error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	return shared.Sweep(s, tcs, s.dialect().BuildSweepQuery)
}

func (s *Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.TempTableID(tableID)
	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)
	return destination.ExecStatements(s, dedupeQueries)
}

func LoadRedshift(cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			configMap: &types.DestinationTableConfigMap{},
			config:    cfg,

			Store: *_store,
		}, nil
	}

	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=require",
		cfg.Redshift.Host, cfg.Redshift.Port, cfg.Redshift.Username,
		cfg.Redshift.Password, cfg.Redshift.Database)

	store, err := db.Open("pgx", connStr)
	if err != nil {
		return nil, err
	}

	return &Store{
		credentialsClause: cfg.Redshift.CredentialsClause,
		bucket:            cfg.Redshift.Bucket,
		optionalS3Prefix:  cfg.Redshift.OptionalS3Prefix,
		configMap:         &types.DestinationTableConfigMap{},
		config:            cfg,

		Store: store,
	}, nil
}
