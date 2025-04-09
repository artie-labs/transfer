package snowflake

import (
	"context"
	"fmt"

	"github.com/snowflakedb/gosnowflake"

	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/sql"
)

type Store struct {
	db.Store
	configMap *types.DestinationTableConfigMap
	config    config.Config
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return dialect.NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s *Store) DropTable(ctx context.Context, tableID sql.TableIdentifier) error {
	if !tableID.AllowToDrop() {
		return fmt.Errorf("table %q is not allowed to be dropped", tableID.FullyQualifiedName())
	}

	if _, err := s.ExecContext(ctx, s.dialect().BuildDropTableQuery(tableID)); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// We'll then clear it from our cache
	s.configMap.RemoveTable(tableID)
	return nil
}

func (s *Store) GetTableConfig(tableID sql.TableIdentifier, dropDeletedColumns bool) (*types.DestinationTableConfig, error) {
	return shared.GetTableCfgArgs{
		Destination:           s,
		TableID:               tableID,
		ConfigMap:             s.configMap,
		ColumnNameForName:     "name",
		ColumnNameForDataType: "type",
		ColumnNameForComment:  "comment",
		DropDeletedColumns:    dropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) SweepTemporaryTables(ctx context.Context) error {
	tcs, err := s.config.TopicConfigs()
	if err != nil {
		return err
	}

	return shared.Sweep(ctx, s, tcs, s.dialect().BuildSweepQuery)
}

func (s *Store) Dialect() sql.Dialect {
	return s.dialect()
}

func (s *Store) dialect() dialect.SnowflakeDialect {
	return dialect.SnowflakeDialect{}
}

func (s *Store) GetConfigMap() *types.DestinationTableConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

// Dedupe takes a table and will remove duplicates based on the primary key(s).
// These queries are inspired and modified from: https://stackoverflow.com/a/71515946
func (s *Store) Dedupe(ctx context.Context, tableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) error {
	stagingTableID := shared.TempTableID(tableID)
	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, includeArtieUpdatedAt)
	return destination.ExecContextStatements(ctx, s, dedupeQueries)
}

func LoadSnowflake(ctx context.Context, cfg config.Config, _store *db.Store) (*Store, error) {
	if _store != nil {
		// Used for tests.
		return &Store{
			configMap: &types.DestinationTableConfigMap{},
			config:    cfg,
			Store:     *_store,
		}, nil
	}

	snowflakeCfg, err := cfg.Snowflake.ToConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Snowflake config: %w", err)
	}

	dsn, err := gosnowflake.DSN(snowflakeCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get Snowflake DSN: %w", err)
	}

	store, err := db.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}

	s := &Store{
		configMap: &types.DestinationTableConfigMap{},
		config:    cfg,
		Store:     store,
	}

	// Set up external stage if configured
	if err := s.setupExternalStage(ctx); err != nil {
		return nil, fmt.Errorf("failed to set up external stage: %w", err)
	}

	return s, nil
}

// setupExternalStage creates and configures the external stage if specified in the config
func (s *Store) setupExternalStage(ctx context.Context) error {
	if !s.useExternalStage() {
		return nil
	}

	// Ensure the external stage exists
	if _, err := s.QueryContext(ctx, fmt.Sprintf(`DESCRIBE STAGE %s`, s.config.Snowflake.ExternalStage.ExternalStageName)); err != nil {
		return fmt.Errorf("failed to describe external stage: %w", err)
	}

	// Create the external stage
	createStageQuery := fmt.Sprintf(`
		CREATE OR REPLACE STAGE artie_external_stage
		URL = 's3://%s/%s'
		CREDENTIALS = (
			AWS_KEY_ID = '%s'
			AWS_SECRET_KEY = '%s'
		)
		FILE_FORMAT = (
			TYPE = 'CSV'
			FIELD_DELIMITER = '\t'
			FIELD_OPTIONALLY_ENCLOSED_BY = '"'
			NULL_IF = '__artie_null_value'
			EMPTY_FIELD_AS_NULL = FALSE
		)
	`, s3Config.Bucket, s3Config.Prefix, s3Config.AwsAccessKeyID, s3Config.AwsSecretAccessKey)

	if _, err := s.ExecContext(context.Background(), createStageQuery); err != nil {
		return fmt.Errorf("failed to create external stage: %w", err)
	}

	return nil
}
